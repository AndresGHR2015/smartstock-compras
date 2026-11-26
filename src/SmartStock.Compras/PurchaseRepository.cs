using System.Globalization;
using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;

namespace SmartStock.Compras;

public sealed class PurchaseRepository(NpgsqlDataSource dataSource)
{
    private static readonly HashSet<string> CurrencyCodes = CultureInfo
        .GetCultures(CultureTypes.SpecificCultures)
        .Select(culture => new RegionInfo(culture.Name).ISOCurrencySymbol)
        .ToHashSet(StringComparer.Ordinal);

    public async Task<string> Process(
        PurchaseRequest request,
        PurchaseDecisionPolicy? decisionPolicy = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var key = PurchaseContract.IdentityKey(request.RequestId);
        await Lock(connection, transaction, $"request:{key}", cancellationToken);
        var document = request.Document.ToJsonString();

        await using (var command = Command(connection, transaction, """
            SELECT request_id, request_document = @document::jsonb, result_json
            FROM purchase_requests WHERE request_key = @key
            """, ("document", document), ("key", key)))
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                if (reader.GetString(0) != request.RequestId || !reader.GetBoolean(1))
                    throw new PurchaseRequestConflictException(
                        "request_id reutilizado con contenido diferente; requiere intervención.");

                return reader.GetString(2);
            }
        }

        var operation = await Execute(request, decisionPolicy, connection, transaction, cancellationToken);
        var result = PurchaseContract.CreateResult(request, operation.Outcome, operation.Data);
        await using var insert = Command(connection, transaction, """
            INSERT INTO purchase_requests (request_key, request_id, request_document, result_json)
            VALUES (@key, @id, @document::jsonb, @result)
            """, ("key", key), ("id", request.RequestId), ("document", document), ("result", result));
        await insert.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    private static async Task<OperationResult> Execute(
        PurchaseRequest request,
        PurchaseDecisionPolicy? decisionPolicy,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var payload = request.Payload;
        switch (request.Type)
        {
            case "get_overview":
                return new OperationResult("OK", await Overview(connection, transaction, cancellationToken));
            case "get_need":
                return Found(await ReadNeed(connection, transaction, Text(payload, "event_id"), cancellationToken));
            case "list_needs":
            case "list_offers":
                return await List(request, connection, transaction, cancellationToken);
            case "get_order":
                return Found(await ReadOrder(connection, transaction, Text(payload, "order_id"), cancellationToken));
            case "create_draft":
                return await CreateDraft(payload, connection, transaction, cancellationToken);
            case "decide_order":
                if (decisionPolicy?.CanDecide(Text(payload, "actor_id")) != true)
                    return new OperationResult("FORBIDDEN");
                return await Decide(payload, connection, transaction, cancellationToken);
            default:
                throw new FormatException("Operación de Compras desconocida.");
        }
    }

    private static async Task<JsonObject> Overview(
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, """
            WITH recent AS (
                SELECT event_id, sku, occurred_at FROM replenishment_needs
                ORDER BY occurred_at DESC, event_id LIMIT 5
            )
            SELECT
                (SELECT count(*)::integer FROM replenishment_needs) AS pending_count,
                (SELECT count(*)::integer FROM purchase_orders WHERE status IN ('DRAFT', 'APPROVED')) AS open_order_count,
                COALESCE((
                    SELECT jsonb_agg(jsonb_build_object(
                        'event_id', event_id, 'sku', sku,
                        'occurred_at', to_char(occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                        'status', coalesce((
                            SELECT status FROM purchase_orders
                            WHERE need_event_id = recent.event_id AND status IN ('DRAFT', 'APPROVED')
                            ORDER BY (status = 'APPROVED') DESC LIMIT 1
                        ), 'PENDING')
                    ) ORDER BY occurred_at DESC, event_id) FROM recent
                ), '[]'::jsonb) AS needs
            """);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new JsonObject
        {
            ["pending_count"] = reader.GetInt32(0),
            ["open_order_count"] = reader.GetInt32(1),
            ["needs"] = JsonNode.Parse(reader.GetString(2))
        };
    }

    private static async Task<OperationResult> List(
        PurchaseRequest request,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var payload = request.Payload;
        var sku = Text(payload, "sku");
        var limit = Number(payload, "limit");
        string? after = null;
        if (payload["cursor"] is not null)
        {
            await using var cursor = Command(connection, transaction, """
                SELECT after_id FROM purchase_cursors
                WHERE token = @token AND operation = @operation AND sku = @sku
                """, ("token", Text(payload, "cursor")), ("operation", request.Type), ("sku", sku));
            after = (string?)await cursor.ExecuteScalarAsync(cancellationToken);
            if (after is null)
                return new OperationResult("INVALID_ARGUMENT");
        }

        var sql = request.Type == "list_needs"
            ? """
              SELECT jsonb_build_object('event_id', event_id, 'sku', sku,
                  'current_stock', current_stock, 'threshold', threshold)::text
              FROM replenishment_needs
              WHERE sku = @sku AND (@after IS NULL OR event_id COLLATE "C" > @after COLLATE "C")
              ORDER BY event_id COLLATE "C" LIMIT @limit
              """
            : """
              SELECT jsonb_build_object('offer_id', id, 'supplier_id', supplier_id, 'sku', sku,
                  'unit_price', unit_price::text, 'currency', currency,
                  'minimum_quantity', minimum_quantity, 'lead_time_days', lead_time_days)::text
              FROM quotations
              WHERE sku = @sku AND (@after IS NULL OR id COLLATE "C" > @after COLLATE "C")
              ORDER BY id COLLATE "C" LIMIT @limit
              """;
        var items = new JsonArray();
        await using (var command = Command(connection, transaction, sql,
            ("sku", sku), ("after", after), ("limit", limit + 1)))
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                items.Add(JsonNode.Parse(reader.GetString(0)));
        }

        string? nextCursor = null;
        if (items.Count > limit)
        {
            items.RemoveAt(items.Count - 1);
            var idName = request.Type == "list_needs" ? "event_id" : "offer_id";
            var lastId = items[^1]![idName]!.GetValue<string>();
            nextCursor = Guid.NewGuid().ToString("N");
            await using var insert = Command(connection, transaction, """
                INSERT INTO purchase_cursors (token, operation, sku, after_id)
                VALUES (@token, @operation, @sku, @after)
                """, ("token", nextCursor), ("operation", request.Type), ("sku", sku), ("after", lastId));
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        return new OperationResult("OK", new JsonObject
        {
            ["items"] = items,
            ["next_cursor"] = nextCursor
        });
    }

    private static async Task<OperationResult> CreateDraft(
        JsonObject payload,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var orderId = Text(payload, "order_id");
        await Lock(connection, transaction, $"order:{PurchaseContract.IdentityKey(orderId)}", cancellationToken);
        if (await ReadOrder(connection, transaction, orderId, cancellationToken) is not null)
            return new OperationResult("CONFLICT");

        var needId = Text(payload, "need_event_id");
        var need = await ReadNeed(connection, transaction, needId, cancellationToken);
        if (need is null)
            return new OperationResult("NOT_FOUND");

        JsonObject? offer;
        await using (var command = Command(connection, transaction, """
            SELECT jsonb_build_object('offer_id', id, 'supplier_id', supplier_id, 'sku', sku,
                'unit_price', unit_price::text, 'currency', currency,
                'minimum_quantity', minimum_quantity, 'lead_time_days', lead_time_days)::text
            FROM quotations WHERE id = @id
            """, ("id", Text(payload, "offer_id"))))
        {
            var json = (string?)await command.ExecuteScalarAsync(cancellationToken);
            offer = json is null ? null : (JsonObject)JsonNode.Parse(json)!;
        }
        if (offer is null)
            return new OperationResult("NOT_FOUND");

        var quantity = Number(payload, "quantity");
        if (!PurchaseContract.IsValidOffer(offer)
            || Text(need, "sku") != Text(offer, "sku")
            || quantity < Number(offer, "minimum_quantity")
            || !CurrencyCodes.Contains(Text(offer, "currency")))
        {
            return new OperationResult("INVALID_ARGUMENT");
        }

        await using var insert = Command(connection, transaction, """
            INSERT INTO purchase_orders
                (order_key, order_id, need_event_id, offer, quantity, status, revision)
            VALUES (@key, @id, @need, @offer::jsonb, @quantity, 'DRAFT', 1)
            """, ("key", PurchaseContract.IdentityKey(orderId)), ("id", orderId),
            ("need", needId), ("offer", offer.ToJsonString()), ("quantity", quantity));
        await insert.ExecuteNonQueryAsync(cancellationToken);
        return Found(await ReadOrder(connection, transaction, orderId, cancellationToken));
    }

    private static async Task<OperationResult> Decide(
        JsonObject payload,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var orderId = Text(payload, "order_id");
        var key = PurchaseContract.IdentityKey(orderId);
        await Lock(connection, transaction, $"order:{key}", cancellationToken);
        var order = await ReadOrder(connection, transaction, orderId, cancellationToken);
        if (order is null)
            return new OperationResult("NOT_FOUND");
        if (Text(order, "status") != "DRAFT" || Number(order, "revision") != Number(payload, "expected_revision"))
            return new OperationResult("CONFLICT");

        var status = Text(payload, "decision") == "APPROVE" ? "APPROVED" : "REJECTED";
        await using var update = Command(connection, transaction, """
            UPDATE purchase_orders SET status = @status, revision = revision + 1, decided_by = @actor
            WHERE order_key = @key AND status = 'DRAFT' AND revision = @revision
            """, ("status", status), ("actor", Text(payload, "actor_id")),
            ("key", key), ("revision", Number(payload, "expected_revision")));
        if (await update.ExecuteNonQueryAsync(cancellationToken) != 1)
            return new OperationResult("CONFLICT");
        return Found(await ReadOrder(connection, transaction, orderId, cancellationToken));
    }

    private static async Task<JsonObject?> ReadNeed(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string eventId,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, """
            SELECT jsonb_build_object('event_id', event_id, 'sku', sku,
                'current_stock', current_stock, 'threshold', threshold)::text
            FROM replenishment_needs WHERE event_id = @id
            """, ("id", eventId));
        var json = (string?)await command.ExecuteScalarAsync(cancellationToken);
        return json is null ? null : (JsonObject)JsonNode.Parse(json)!;
    }

    private static async Task<JsonObject?> ReadOrder(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string orderId,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, """
            SELECT order_id, jsonb_build_object('order_id', order_id, 'need_event_id', need_event_id,
                'offer', offer, 'quantity', quantity, 'status', status,
                'revision', revision, 'decided_by', decided_by)::text
            FROM purchase_orders WHERE order_key = @key
            """, ("key", PurchaseContract.IdentityKey(orderId)));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;
        if (reader.GetString(0) != orderId)
            throw new PurchaseRequestConflictException("Colisión de identidad de orden; requiere intervención.");
        return (JsonObject)JsonNode.Parse(reader.GetString(1))!;
    }

    private static async Task Lock(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string identity,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction,
            "SELECT pg_advisory_xact_lock(hashtextextended(@identity, 0))", ("identity", identity));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static NpgsqlCommand Command(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string sql,
        params (string Name, object? Value)[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction);
        foreach (var parameter in parameters)
        {
            if (parameter.Value is null)
                command.Parameters.Add(parameter.Name, NpgsqlDbType.Text).Value = DBNull.Value;
            else
                command.Parameters.AddWithValue(parameter.Name, parameter.Value);
        }
        return command;
    }

    private static string Text(JsonObject payload, string name) => payload[name]!.GetValue<string>();
    private static int Number(JsonObject payload, string name) => decimal.ToInt32(payload[name]!.GetValue<decimal>());
    private static OperationResult Found(JsonObject? data) => new(data is null ? "NOT_FOUND" : "OK", data);
    private sealed record OperationResult(string Outcome, JsonNode? Data = null);
}
