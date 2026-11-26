using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using SmartStock.Compras;
using Xunit;
using static SmartStock.Compras.Tests.PurchaseContractTests;

namespace SmartStock.Compras.Tests;

public class PurchaseKafkaTests
{
    private static string Bootstrap => Environment.GetEnvironmentVariable("TEST_KAFKA_BOOTSTRAP_SERVERS")
        ?? throw new InvalidOperationException("TEST_KAFKA_BOOTSTRAP_SERVERS es obligatorio.");

    private static NpgsqlDataSource DataSource(int commandTimeout = 30)
    {
        var builder = new NpgsqlConnectionStringBuilder(
            Environment.GetEnvironmentVariable("TEST_DATABASE_URL")
            ?? throw new InvalidOperationException("TEST_DATABASE_URL es obligatorio."))
        {
            CommandTimeout = commandTimeout
        };
        return NpgsqlDataSource.Create(builder.ConnectionString);
    }

    private static IConsumer<string, string> Observer(string group) =>
        new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = Bootstrap,
            GroupId = group,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

    private static IProducer<string, string> Producer() =>
        new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = Bootstrap }).Build();

    private static async Task EnsureTopics()
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        foreach (var name in new[] { PurchaseRequestConsumer.RequestsTopic, PurchaseRequestConsumer.ResultsTopic })
        {
            try
            {
                await admin.CreateTopicsAsync([new TopicSpecification
                {
                    Name = name,
                    NumPartitions = 1,
                    ReplicationFactor = 1
                }]);
            }
            catch (CreateTopicsException exception) when (
                exception.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists))
            {
            }
        }
    }

    private static PurchaseRequestConsumer Worker(NpgsqlDataSource dataSource, string group)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KAFKA_BOOTSTRAP_SERVERS"] = Bootstrap,
            ["PURCHASES_KAFKA_GROUP_ID"] = group
        }).Build();
        return new PurchaseRequestConsumer(new NeedRepository(dataSource), new PurchaseRepository(dataSource),
            configuration, NullLogger<PurchaseRequestConsumer>.Instance);
    }

    private static void StartGroupAt(string group, DeliveryResult<string, string> message)
    {
        using var observer = Observer(group);
        observer.Assign(message.TopicPartitionOffset);
        observer.Commit([message.TopicPartitionOffset]);
        observer.Close();
    }

    private static async Task WaitForCommit(
        string group, DeliveryResult<string, string> message, CancellationToken cancellationToken)
    {
        using var observer = Observer(group);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var offset = observer.Committed([message.TopicPartition], TimeSpan.FromSeconds(5))[0].Offset;
            if (offset.Value > message.Offset.Value)
                return;
            await Task.Delay(100, cancellationToken);
        }
    }

    [Fact]
    public async Task RestartBeforePublicationAndRedeliveryPublishExactDurableResultsBeforeCommitting()
    {
        await EnsureTopics();
        await using var dataSource = DataSource();
        await new NeedRepository(dataSource).Migrate(CancellationToken.None);
        var request = Request("get_order", new { order_id = Guid.NewGuid().ToString("N") });
        var persistedBeforeCrash = await new PurchaseRepository(dataSource).Process(request);
        using var producer = Producer();
        var first = await producer.ProduceAsync(PurchaseRequestConsumer.RequestsTopic, new Message<string, string>
        {
            Key = request.RequestId,
            Value = request.Document.ToJsonString()
        });
        var second = await producer.ProduceAsync(PurchaseRequestConsumer.RequestsTopic, new Message<string, string>
        {
            Key = request.RequestId,
            Value = request.Document.ToJsonString()
        });
        var group = $"purchases-recovery-{Guid.NewGuid():N}";
        StartGroupAt(group, first);
        using var results = Observer($"purchases-results-{Guid.NewGuid():N}");
        results.Subscribe(PurchaseRequestConsumer.ResultsTopic);
        using var worker = Worker(dataSource, group);
        await worker.StartAsync(CancellationToken.None);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        try
        {
            var received = new List<string>();
            while (received.Count < 2)
            {
                var message = results.Consume(deadline.Token);
                if (message.Message.Key == request.RequestId)
                    received.Add(message.Message.Value);
            }
            Assert.All(received, result => Assert.Equal(persistedBeforeCrash, result));
            await WaitForCommit(group, second, deadline.Token);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
            results.Close();
        }
    }

    [Theory]
    [InlineData("key")]
    [InlineData("schema")]
    [InlineData("conflict")]
    public async Task InvalidOrConflictingRequestsStopWithoutAdvancingOffset(string scenario)
    {
        await EnsureTopics();
        await using var dataSource = DataSource();
        await new NeedRepository(dataSource).Migrate(CancellationToken.None);
        var request = Request("get_need", new { event_id = Guid.NewGuid().ToString("N") });
        if (scenario == "conflict")
        {
            await new PurchaseRepository(dataSource).Process(request);
            request.Document["payload"]!["event_id"] = "changed";
        }
        if (scenario == "schema")
            request.Document["extra"] = true;

        using var producer = Producer();
        var published = await producer.ProduceAsync(PurchaseRequestConsumer.RequestsTopic, new Message<string, string>
        {
            Key = scenario == "key" ? "wrong-key" : request.RequestId,
            Value = request.Document.ToJsonString()
        });
        var group = $"purchases-fatal-{Guid.NewGuid():N}";
        StartGroupAt(group, published);
        using var worker = Worker(dataSource, group);
        await worker.StartAsync(CancellationToken.None);
        try
        {
            var exception = await Record.ExceptionAsync(async () =>
                await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30)));
            if (scenario == "conflict")
                Assert.IsType<PurchaseRequestConflictException>(exception);
            else
                Assert.IsType<FormatException>(exception);
            using var observer = Observer(group);
            var offset = observer.Committed([published.TopicPartition], TimeSpan.FromSeconds(5))[0].Offset;
            Assert.Equal(published.Offset.Value, offset.Value);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task DatabaseTimeoutRetriesWithoutAdvancingRequestOffset()
    {
        await EnsureTopics();
        await using var dataSource = DataSource(commandTimeout: 1);
        await new NeedRepository(dataSource).Migrate(CancellationToken.None);
        var request = Request("get_order", new { order_id = Guid.NewGuid().ToString("N") });
        using var producer = Producer();
        var published = await producer.ProduceAsync(PurchaseRequestConsumer.RequestsTopic, new Message<string, string>
        {
            Key = request.RequestId,
            Value = request.Document.ToJsonString()
        });
        var group = $"purchases-retry-{Guid.NewGuid():N}";
        StartGroupAt(group, published);
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var tableLock = new NpgsqlCommand(
            "LOCK TABLE purchase_requests IN ACCESS EXCLUSIVE MODE", connection, transaction);
        await tableLock.ExecuteNonQueryAsync();
        using var worker = Worker(dataSource, group);
        await worker.StartAsync(CancellationToken.None);
        var released = false;
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(3));
            using var observer = Observer(group);
            var offset = observer.Committed([published.TopicPartition], TimeSpan.FromSeconds(5))[0].Offset;
            Assert.Equal(published.Offset.Value, offset.Value);
            await transaction.CommitAsync();
            released = true;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            await WaitForCommit(group, published, deadline.Token);
            await using var command = dataSource.CreateCommand(
                "SELECT count(*) FROM purchase_requests WHERE request_key = @key");
            command.Parameters.AddWithValue("key", PurchaseContract.IdentityKey(request.RequestId));
            Assert.Equal(1L, await command.ExecuteScalarAsync());
        }
        finally
        {
            if (!released)
                await transaction.RollbackAsync();
            await worker.StopAsync(CancellationToken.None);
        }
    }
}
