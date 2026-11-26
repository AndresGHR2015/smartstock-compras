using Confluent.Kafka;
using Npgsql;
using SmartStock.Compras;

var builder = Host.CreateApplicationBuilder(args);
var connectionString = builder.Configuration["DATABASE_URL"]
    ?? throw new InvalidOperationException("DATABASE_URL es obligatorio.");
if (args.Length == 2 && args[0] == "--inspect")
{
    await using var dataSource = NpgsqlDataSource.Create(connectionString);
    await using var command = dataSource.CreateCommand("""
        SELECT event_id, sku, previous_stock, current_stock, threshold, sale_id,
               occurred_at, kafka_topic, kafka_partition, kafka_offset
        FROM replenishment_needs WHERE event_id = @event_id
        """);
    command.Parameters.AddWithValue("event_id", args[1]);
    await using var reader = await command.ExecuteReaderAsync();
    if (!await reader.ReadAsync())
    {
        Console.Error.WriteLine("Necesidad no encontrada.");
        Environment.ExitCode = 1;
        return;
    }
    var result = new Dictionary<string, object>();
    for (var index = 0; index < reader.FieldCount; index++)
        result.Add(reader.GetName(index), reader.GetValue(index));
    Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(result));
    return;
}
builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton<NeedRepository>();
builder.Services.AddSingleton<PurchaseRepository>();
builder.Services.AddHostedService<StockLowConsumer>();
builder.Services.AddHostedService<PurchaseRequestConsumer>();
await builder.Build().RunAsync();

public sealed class StockLowConsumer(
    NeedRepository repository, IConfiguration configuration, ILogger<StockLowConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrap = configuration["KAFKA_BOOTSTRAP_SERVERS"]
            ?? throw new InvalidOperationException("KAFKA_BOOTSTRAP_SERVERS es obligatorio.");
        var group = configuration["KAFKA_GROUP_ID"] ?? "smartstock-compras";
        await repository.Migrate(stoppingToken);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Consume(bootstrap, group, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException exception) when (
                exception.Error.Code is ErrorCode.Local_KeyDeserialization or ErrorCode.Local_ValueDeserialization)
            {
                logger.LogCritical(exception, "Evento Kafka no decodificable; requiere intervención sin confirmar.");
                throw;
            }
            catch (KafkaException exception)
            {
                logger.LogError(exception, "Fallo de transporte Kafka; se recreará el consumidor desde offsets confirmados.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task Consume(string bootstrap, string group, CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = group,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe("inventory.stock_low.v1");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var message = consumer.Consume(stoppingToken);
                StockLowEvent stockEvent;
                try
                {
                    stockEvent = StockLowEvent.Parse(message.Message.Value);
                    if (message.Message.Key != stockEvent.Sku)
                        throw new FormatException("La clave Kafka no coincide con el SKU.");
                }
                catch (FormatException exception)
                {
                    logger.LogCritical(exception, "Mensaje inválido en {Position}; offset no confirmado. Requiere intervención.",
                        message.TopicPartitionOffset);
                    throw;
                }

                while (true)
                {
                    try
                    {
                        var inserted = await repository.Save(stockEvent, message.Topic, message.Partition.Value,
                            message.Offset.Value, stoppingToken);
                        logger.LogInformation("Evento {EventId} persistido; nuevo: {Inserted}", stockEvent.EventId, inserted);
                        consumer.Commit(message);
                        break;
                    }
                    catch (Exception exception) when (exception is NpgsqlException or TimeoutException)
                    {
                        logger.LogError(exception, "Fallo transitorio en {Position}; se reintentará sin descartar.",
                            message.TopicPartitionOffset);
                        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                    }
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
