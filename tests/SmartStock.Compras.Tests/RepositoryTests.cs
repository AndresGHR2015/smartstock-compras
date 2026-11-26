using Npgsql;
using SmartStock.Compras;
using Xunit;

namespace SmartStock.Compras.Tests;

public class RepositoryTests
{
    [Fact]
    public async Task ConcurrentRedeliveryCreatesOneNeed()
    {
        var connectionString = Environment.GetEnvironmentVariable("TEST_DATABASE_URL")
            ?? throw new InvalidOperationException("TEST_DATABASE_URL es obligatorio para pruebas de PostgreSQL.");
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        var repository = new NeedRepository(dataSource);
        await repository.Migrate(CancellationToken.None);
        var stockEvent = StockLowEvent.Parse(StockLowEventTests.Example) with { EventId = Guid.NewGuid().ToString() };

        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(index =>
            repository.Save(stockEvent, "inventory.stock_low.v1", 0, index, CancellationToken.None)));

        Assert.Single(results, inserted => inserted);
        await using var command = dataSource.CreateCommand(
            "SELECT count(*) FROM replenishment_needs WHERE event_id = @event_id");
        command.Parameters.AddWithValue("event_id", stockEvent.EventId);
        Assert.Equal(1L, await command.ExecuteScalarAsync());
    }
}
