using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using SmartStock.Compras;
using Xunit;

namespace SmartStock.Compras.Tests;

public class KafkaTests
{
    [Fact]
    public async Task DatabaseTimeoutDoesNotAdvanceOffsetAndIsRetried()
    {
        var bootstrap = Environment.GetEnvironmentVariable("TEST_KAFKA_BOOTSTRAP_SERVERS")
            ?? throw new InvalidOperationException("TEST_KAFKA_BOOTSTRAP_SERVERS es obligatorio.");
        var connectionString = Environment.GetEnvironmentVariable("TEST_DATABASE_URL")
            ?? throw new InvalidOperationException("TEST_DATABASE_URL es obligatorio.");
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { CommandTimeout = 1 };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var repository = new NeedRepository(dataSource);
        await repository.Migrate(CancellationToken.None);
        var group = $"compras-retry-{Guid.NewGuid():N}";
        var eventId = $"evt-{Guid.NewGuid():N}";
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap,
            ["KAFKA_GROUP_ID"] = group
        }).Build();
        const string topic = "inventory.stock_low.v1";
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrap
        }).Build();
        var message = await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "SKU-001",
            Value = StockLowEventTests.Example.Replace("evt-sale-001", eventId)
        });
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var lockCommand = new NpgsqlCommand(
            "LOCK TABLE replenishment_needs IN ACCESS EXCLUSIVE MODE", connection, transaction);
        await lockCommand.ExecuteNonQueryAsync();
        using var worker = new StockLowConsumer(repository, configuration, NullLogger<StockLowConsumer>.Instance);
        await worker.StartAsync(CancellationToken.None);
        var lockReleased = false;
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(4));
            using var observer = new ConsumerBuilder<string, string>(new ConsumerConfig
            {
                BootstrapServers = bootstrap,
                GroupId = group,
                EnableAutoCommit = false
            }).Build();
            var before = observer.Committed([message.TopicPartition], TimeSpan.FromSeconds(10))[0].Offset;
            Assert.True(before.Value <= message.Offset.Value);
            await transaction.CommitAsync();
            lockReleased = true;

            var deadline = DateTime.UtcNow.AddSeconds(40);
            var after = before;
            while (DateTime.UtcNow < deadline && after.Value <= message.Offset.Value)
            {
                await Task.Delay(250);
                after = observer.Committed([message.TopicPartition], TimeSpan.FromSeconds(5))[0].Offset;
            }
            Assert.True(after.Value > message.Offset.Value);
            await using var command = dataSource.CreateCommand(
                "SELECT count(*) FROM replenishment_needs WHERE event_id = @event_id");
            command.Parameters.AddWithValue("event_id", eventId);
            Assert.Equal(1L, await command.ExecuteScalarAsync());
        }
        finally
        {
            if (!lockReleased)
                await transaction.RollbackAsync();
            await worker.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task RedeliveryIsPersistedOnceAndOffsetsAreCommitted()
    {
        var bootstrap = Environment.GetEnvironmentVariable("TEST_KAFKA_BOOTSTRAP_SERVERS")
            ?? throw new InvalidOperationException("TEST_KAFKA_BOOTSTRAP_SERVERS es obligatorio.");
        var connectionString = Environment.GetEnvironmentVariable("TEST_DATABASE_URL")
            ?? throw new InvalidOperationException("TEST_DATABASE_URL es obligatorio.");
        var group = $"compras-test-{Guid.NewGuid():N}";
        var eventId = $"evt-{Guid.NewGuid():N}";
        var payload = StockLowEventTests.Example.Replace("evt-sale-001", eventId);
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap,
            ["KAFKA_GROUP_ID"] = group
        }).Build();
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        var repository = new NeedRepository(dataSource);
        await repository.Migrate(CancellationToken.None);
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrap
        }).Build();
        const string topic = "inventory.stock_low.v1";
        var first = await producer.ProduceAsync(topic, new Message<string, string> { Key = "SKU-001", Value = payload });
        var second = await producer.ProduceAsync(topic, new Message<string, string> { Key = "SKU-001", Value = payload });

        using var worker = new StockLowConsumer(repository, configuration, NullLogger<StockLowConsumer>.Instance);
        await worker.StartAsync(CancellationToken.None);
        try
        {
            using var observer = new ConsumerBuilder<string, string>(new ConsumerConfig
            {
                BootstrapServers = bootstrap,
                GroupId = group,
                EnableAutoCommit = false
            }).Build();
            var deadline = DateTime.UtcNow.AddSeconds(45);
            var committed = Offset.Unset;
            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    var positions = observer.Committed([second.TopicPartition], TimeSpan.FromSeconds(5));
                    committed = positions[0].Offset;
                    if (committed.Value > second.Offset.Value)
                        break;
                }
                catch (KafkaException)
                {
                    // The broker may still be electing the group coordinator.
                }
                await Task.Delay(200);
            }
            Assert.True(committed.Value > second.Offset.Value, $"Offset confirmado: {committed}.");
            await using var command = dataSource.CreateCommand("""
                SELECT count(*), min(kafka_offset)
                FROM replenishment_needs WHERE event_id = @event_id
                """);
            command.Parameters.AddWithValue("event_id", eventId);
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1L, reader.GetInt64(0));
            Assert.Equal(first.Offset.Value, reader.GetInt64(1));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }
    }
}
