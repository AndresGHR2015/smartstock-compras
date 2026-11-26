using Npgsql;

namespace SmartStock.Compras;

public sealed class NeedRepository(NpgsqlDataSource dataSource)
{
    public async Task Migrate(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var lockCommand = new NpgsqlCommand("SELECT pg_advisory_xact_lock(234567891)", connection, transaction);
        await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        await using var versions = new NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY)", connection, transaction);
        await versions.ExecuteNonQueryAsync(cancellationToken);
        foreach (var version in new[] { "001", "002" })
        {
            await using var check = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = @version)", connection, transaction);
            check.Parameters.AddWithValue("version", version);
            if ((bool)(await check.ExecuteScalarAsync(cancellationToken))!)
                continue;

            var assembly = typeof(NeedRepository).Assembly;
            var resourceName = assembly.GetManifestResourceNames()
                .Single(name => name.Contains($".Migrations.{version}_") && name.EndsWith(".sql"));
            await using var stream = assembly.GetManifestResourceStream(resourceName)!;
            using var reader = new StreamReader(stream);
            var sql = await reader.ReadToEndAsync(cancellationToken);
            await using var migration = new NpgsqlCommand(sql, connection, transaction);
            await migration.ExecuteNonQueryAsync(cancellationToken);
            await using var record = new NpgsqlCommand(
                "INSERT INTO schema_migrations (version) VALUES (@version)", connection, transaction);
            record.Parameters.AddWithValue("version", version);
            await record.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<bool> Save(StockLowEvent stockEvent, string topic, int partition, long offset, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        const string sql = """
            INSERT INTO replenishment_needs
                (event_id, event_type, event_version, occurred_at, sku, previous_stock, current_stock,
                 threshold, sale_id, kafka_topic, kafka_partition, kafka_offset)
            VALUES (@event_id, @event_type, @event_version, @occurred_at, @sku, @previous_stock, @current_stock,
                    @threshold, @sale_id, @topic, @partition, @offset)
            ON CONFLICT (event_id) DO NOTHING
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("event_id", stockEvent.EventId);
        command.Parameters.AddWithValue("event_type", stockEvent.EventType);
        command.Parameters.AddWithValue("event_version", stockEvent.EventVersion);
        command.Parameters.AddWithValue("occurred_at", stockEvent.OccurredAt);
        command.Parameters.AddWithValue("sku", stockEvent.Sku);
        command.Parameters.AddWithValue("previous_stock", stockEvent.PreviousStock);
        command.Parameters.AddWithValue("current_stock", stockEvent.CurrentStock);
        command.Parameters.AddWithValue("threshold", stockEvent.Threshold);
        command.Parameters.AddWithValue("sale_id", stockEvent.SaleId);
        command.Parameters.AddWithValue("topic", topic);
        command.Parameters.AddWithValue("partition", partition);
        command.Parameters.AddWithValue("offset", offset);
        var inserted = await command.ExecuteNonQueryAsync(cancellationToken) == 1;
        await transaction.CommitAsync(cancellationToken);
        return inserted;
    }
}
