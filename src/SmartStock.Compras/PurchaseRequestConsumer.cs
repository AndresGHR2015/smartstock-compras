using Confluent.Kafka;
using Npgsql;

namespace SmartStock.Compras;

public sealed class PurchaseRequestConsumer(
    NeedRepository migrations,
    PurchaseRepository repository,
    IConfiguration configuration,
    ILogger<PurchaseRequestConsumer> logger) : BackgroundService
{
    public const string RequestsTopic = "purchases.requests.v1";
    public const string ResultsTopic = "purchases.results.v1";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrap = configuration["KAFKA_BOOTSTRAP_SERVERS"]
            ?? throw new InvalidOperationException("KAFKA_BOOTSTRAP_SERVERS es obligatorio.");
        var group = configuration["PURCHASES_KAFKA_GROUP_ID"] ?? "smartstock-compras-purchases";
        var decisionPolicy = PurchaseDecisionPolicy.FromConfiguration(configuration);
        if (decisionPolicy.Enabled)
            logger.LogWarning("Decisiones locales habilitadas SIN autenticación verificable. No usar en producción.");

        await migrations.Migrate(stoppingToken);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Consume(bootstrap, group, decisionPolicy, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException exception) when (
                exception.Error.Code is ErrorCode.Local_KeyDeserialization or ErrorCode.Local_ValueDeserialization)
            {
                logger.LogCritical(exception, "Mensaje Kafka no decodificable; requiere intervención sin confirmar.");
                throw;
            }
            catch (KafkaException exception)
            {
                logger.LogError(exception, "Fallo de transporte Kafka; se recreará el consumidor desde offsets confirmados.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task Consume(
        string bootstrap,
        string group,
        PurchaseDecisionPolicy decisionPolicy,
        CancellationToken stoppingToken)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrap,
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = 30000
        }).Build();
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = group,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(RequestsTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var message = consumer.Consume(stoppingToken);
                try
                {
                    var request = PurchaseContract.Parse(message.Message.Key, message.Message.Value);
                    while (true)
                    {
                        try
                        {
                            var result = await repository.Process(request, decisionPolicy, stoppingToken);
                            // The journal is the durable outbox: publish and acknowledge before the request offset.
                            await producer.ProduceAsync(ResultsTopic, new Message<string, string>
                            {
                                Key = request.RequestId,
                                Value = result
                            }, stoppingToken);
                            consumer.Commit(message);
                            logger.LogInformation("Solicitud {RequestId} procesada y resultado entregado.", request.RequestId);
                            break;
                        }
                        catch (Exception exception) when (exception is NpgsqlException or TimeoutException)
                        {
                            logger.LogError(exception, "Fallo técnico en {Position}; se reintentará sin confirmar el offset.",
                                message.TopicPartitionOffset);
                            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                        }
                    }
                }
                catch (Exception exception) when (exception is FormatException or PurchaseRequestConflictException)
                {
                    logger.LogCritical(exception, "Solicitud inválida o conflictiva en {Position}; requiere intervención sin confirmar.",
                        message.TopicPartitionOffset);
                    throw;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }
}
