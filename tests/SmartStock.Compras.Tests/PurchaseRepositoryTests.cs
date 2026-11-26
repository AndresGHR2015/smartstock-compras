using System.Text.Json.Nodes;
using Npgsql;
using SmartStock.Compras;
using Xunit;
using static SmartStock.Compras.Tests.PurchaseContractTests;

namespace SmartStock.Compras.Tests;

public class PurchaseRepositoryTests
{
    private static NpgsqlDataSource DataSource() => NpgsqlDataSource.Create(
        Environment.GetEnvironmentVariable("TEST_DATABASE_URL")
        ?? throw new InvalidOperationException("TEST_DATABASE_URL es obligatorio."));

    public static async Task<string> SeedNeed(NpgsqlDataSource dataSource, string sku = "SKU-001")
    {
        var repository = new NeedRepository(dataSource);
        await repository.Migrate(CancellationToken.None);
        var eventId = $"evt-{Guid.NewGuid():N}";
        var stockEvent = StockLowEvent.Parse(StockLowEventTests.Example) with { EventId = eventId, Sku = sku };
        await repository.Save(stockEvent, "inventory.stock_low.v1", 0, 0, CancellationToken.None);
        return eventId;
    }

    public static async Task<JsonObject> Execute(
        PurchaseRepository repository, string type, object payload, PurchaseDecisionPolicy? policy = null)
    {
        var json = await repository.Process(Request(type, payload), policy);
        var result = (JsonObject)JsonNode.Parse(json)!;
        PurchaseContract.ValidateResult(result);
        return result;
    }

    private static void Outcome(JsonObject result, string expected) =>
        Assert.Equal(expected, result["outcome"]!.GetValue<string>());

    [Fact]
    public async Task AllOperationsReturnContractValidResultsAndSnapshotOffer()
    {
        await using var dataSource = DataSource();
        var eventId = await SeedNeed(dataSource);
        var repository = new PurchaseRepository(dataSource);
        var orderId = $"order-{Guid.NewGuid():N}";
        var need = await Execute(repository, "get_need", new { event_id = eventId });
        Outcome(need, "OK");
        Assert.Equal(eventId, need["data"]!["event_id"]!.GetValue<string>());
        Outcome(await Execute(repository, "get_need", new { event_id = "missing" }), "NOT_FOUND");
        Outcome(await Execute(repository, "get_order", new { order_id = orderId }), "NOT_FOUND");
        Outcome(await Execute(repository, "list_needs", new { sku = "SKU-001", limit = 100, cursor = (string?)null }), "OK");
        Outcome(await Execute(repository, "list_offers", new { sku = "SKU-001", limit = 100, cursor = (string?)null }), "OK");

        var draft = await Execute(repository, "create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 10
        });
        Outcome(draft, "OK");
        Assert.Equal("DRAFT", draft["data"]!["status"]!.GetValue<string>());
        Assert.Equal(1, draft["data"]!["revision"]!.GetValue<int>());
        Assert.Null(draft["data"]!["decided_by"]);
        var snapshot = draft["data"]!["offer"]!.ToJsonString();

        await using (var update = dataSource.CreateCommand("UPDATE quotations SET unit_price = 999.99 WHERE id = 'quote-a'"))
            await update.ExecuteNonQueryAsync();
        try
        {
            var fetched = await Execute(repository, "get_order", new { order_id = orderId });
            Assert.Equal(snapshot, fetched["data"]!["offer"]!.ToJsonString());
        }
        finally
        {
            await using var restore = dataSource.CreateCommand("UPDATE quotations SET unit_price = 12.00 WHERE id = 'quote-a'");
            await restore.ExecuteNonQueryAsync();
        }

        var decision = new { order_id = orderId, expected_revision = 1, decision = "APPROVE", actor_id = "local-operator" };
        Outcome(await Execute(repository, "decide_order", decision), "FORBIDDEN");
        Outcome(await Execute(repository, "decide_order", decision, Policy("production", "true")), "FORBIDDEN");
        Outcome(await Execute(repository, "decide_order", decision, Policy("local", "false")), "FORBIDDEN");
        Outcome(await Execute(repository, "decide_order", decision, Policy("local", "true", "another")), "FORBIDDEN");
        var approved = await Execute(repository, "decide_order", decision, Policy());
        Outcome(approved, "OK");
        Assert.Equal("APPROVED", approved["data"]!["status"]!.GetValue<string>());
        Assert.Equal(2, approved["data"]!["revision"]!.GetValue<int>());
        Assert.Equal("local-operator", approved["data"]!["decided_by"]!.GetValue<string>());
        Outcome(await Execute(repository, "decide_order", decision, Policy()), "CONFLICT");
    }

    [Fact]
    public async Task DraftsValidateReferencesMinimumSkuCurrencyAndOrderReuse()
    {
        await using var dataSource = DataSource();
        var eventId = await SeedNeed(dataSource);
        var otherSkuEvent = await SeedNeed(dataSource, "ANOTHER-SKU");
        var repository = new PurchaseRepository(dataSource);
        var orderId = Guid.NewGuid().ToString("N");
        Outcome(await Execute(repository, "create_draft", new
        {
            order_id = orderId, need_event_id = "missing", offer_id = "quote-a", quantity = 10
        }), "NOT_FOUND");
        Outcome(await Execute(repository, "create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "missing", quantity = 10
        }), "NOT_FOUND");
        Outcome(await Execute(repository, "create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 9
        }), "INVALID_ARGUMENT");
        Outcome(await Execute(repository, "create_draft", new
        {
            order_id = orderId, need_event_id = otherSkuEvent, offer_id = "quote-a", quantity = 10
        }), "INVALID_ARGUMENT");
        await using (var update = dataSource.CreateCommand("UPDATE quotations SET currency = 'ZZZ' WHERE id = 'quote-a'"))
            await update.ExecuteNonQueryAsync();
        try
        {
            Outcome(await Execute(repository, "create_draft", new
            {
                order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 10
            }), "INVALID_ARGUMENT");
        }
        finally
        {
            await using var restore = dataSource.CreateCommand("UPDATE quotations SET currency = 'USD' WHERE id = 'quote-a'");
            await restore.ExecuteNonQueryAsync();
        }
        var valid = new { order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 10 };
        Outcome(await Execute(repository, "create_draft", valid), "OK");
        Outcome(await Execute(repository, "create_draft", valid), "CONFLICT");
    }

    [Fact]
    public async Task ConcurrentDuplicatesReplayExactResultAfterRestartAndChangedContentIsFatal()
    {
        await using var dataSource = DataSource();
        var eventId = await SeedNeed(dataSource);
        var repository = new PurchaseRepository(dataSource);
        var orderId = Guid.NewGuid().ToString("N");
        var request = Request("create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 10
        });
        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => repository.Process(request)));
        Assert.All(results, result => Assert.Equal(results[0], result));
        var restarted = new PurchaseRepository(dataSource);
        Assert.Equal(results[0], await restarted.Process(request));

        var reordered = new JsonObject();
        foreach (var pair in request.Document.Reverse())
            reordered.Add(pair.Key, pair.Value?.DeepClone());
        var equivalent = PurchaseContract.Parse(request.RequestId, reordered.ToJsonString());
        Assert.Equal(results[0], await restarted.Process(equivalent));

        request.Document["payload"]!["quantity"] = 20;
        await Assert.ThrowsAsync<PurchaseRequestConflictException>(() => restarted.Process(request));
        request.Document["payload"]!["quantity"] = 10;
        request.Document["occurred_at"] = "2026-11-25T00:00:01Z";
        await Assert.ThrowsAsync<PurchaseRequestConflictException>(() => restarted.Process(request));
        var order = await Execute(repository, "get_order", new { order_id = orderId });
        Assert.Equal(10, order["data"]!["quantity"]!.GetValue<int>());
        await using var command = dataSource.CreateCommand("SELECT count(*) FROM purchase_orders WHERE order_id = @id");
        command.Parameters.AddWithValue("id", orderId);
        Assert.Equal(1L, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ReadAndRejectionResultsStayImmutableWhenBusinessStateChanges()
    {
        await using var dataSource = DataSource();
        await new NeedRepository(dataSource).Migrate(CancellationToken.None);
        var repository = new PurchaseRepository(dataSource);
        var orderId = Guid.NewGuid().ToString("N");
        var read = Request("get_order", new { order_id = orderId });
        var absent = await repository.Process(read);
        var eventId = await SeedNeed(dataSource);
        Outcome(await Execute(repository, "create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 10
        }), "OK");
        Assert.Equal(absent, await repository.Process(read));
        var decision = Request("decide_order", new
        {
            order_id = orderId, expected_revision = 1, decision = "REJECT", actor_id = "local-operator"
        });
        var forbidden = await repository.Process(decision);
        Assert.Equal(forbidden, await repository.Process(decision, Policy()));
        Outcome(await Execute(repository, "get_order", new { order_id = orderId }), "OK");
    }

    [Fact]
    public async Task ConcurrentDecisionsHaveOnlyOneWinnerAndReplayTheirOriginalResults()
    {
        await using var dataSource = DataSource();
        var eventId = await SeedNeed(dataSource);
        var repository = new PurchaseRepository(dataSource);
        var orderId = Guid.NewGuid().ToString("N");
        await Execute(repository, "create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 10
        });
        Outcome(await Execute(repository, "decide_order", new
        {
            order_id = orderId, expected_revision = 2, decision = "APPROVE", actor_id = "local-operator"
        }, Policy()), "CONFLICT");
        var requests = new[] { "APPROVE", "REJECT" }.Select(decision => Request("decide_order", new
        {
            order_id = orderId, expected_revision = 1, decision, actor_id = "local-operator"
        })).ToArray();
        var results = await Task.WhenAll(requests.Select(request => repository.Process(request, Policy())));
        Assert.Single(results, result => JsonNode.Parse(result)!["outcome"]!.GetValue<string>() == "OK");
        Assert.Single(results, result => JsonNode.Parse(result)!["outcome"]!.GetValue<string>() == "CONFLICT");
        for (var index = 0; index < requests.Length; index++)
            Assert.Equal(results[index], await repository.Process(requests[index], Policy()));
        var order = await Execute(repository, "get_order", new { order_id = orderId });
        Assert.Equal(2, order["data"]!["revision"]!.GetValue<int>());
    }

    [Fact]
    public async Task PaginationIsOrderedBoundedAndCursorIsContextBound()
    {
        await using var dataSource = DataSource();
        var sku = $"sku-{Guid.NewGuid():N}";
        var ids = new List<string>();
        for (var index = 0; index < 3; index++)
            ids.Add(await SeedNeed(dataSource, sku));
        ids.Sort(StringComparer.Ordinal);
        var repository = new PurchaseRepository(dataSource);
        var firstRequest = Request("list_needs", new { sku, limit = 2, cursor = (string?)null });
        var firstJson = await repository.Process(firstRequest);
        var first = JsonNode.Parse(firstJson)!;
        var items = first["data"]!["items"]!.AsArray();
        Assert.Equal(ids.Take(2), items.Select(item => item!["event_id"]!.GetValue<string>()));
        var cursor = first["data"]!["next_cursor"]!.GetValue<string>();
        var second = await Execute(repository, "list_needs", new { sku, limit = 2, cursor });
        Assert.Single(second["data"]!["items"]!.AsArray());
        Assert.Equal(ids[2], second["data"]!["items"]![0]!["event_id"]!.GetValue<string>());
        Assert.Null(second["data"]!["next_cursor"]);
        Outcome(await Execute(repository, "list_needs", new { sku = "other-sku", limit = 2, cursor }), "INVALID_ARGUMENT");
        Outcome(await Execute(repository, "list_offers", new { sku, limit = 2, cursor }), "INVALID_ARGUMENT");
        Outcome(await Execute(repository, "list_needs", new { sku, limit = 2, cursor = cursor + "x" }), "INVALID_ARGUMENT");
        Assert.Equal(firstJson, await repository.Process(firstRequest));
        var empty = await Execute(repository, "list_needs", new { sku = "absent", limit = 2, cursor = (string?)null });
        Assert.Empty(empty["data"]!["items"]!.AsArray());
        Assert.Null(empty["data"]!["next_cursor"]);

        var offers = await Execute(repository, "list_offers", new { sku = "SKU-001", limit = 2, cursor = (string?)null });
        var offerCursor = offers["data"]!["next_cursor"]!.GetValue<string>();
        Assert.Equal(new[] { "quote-a", "quote-b" },
            offers["data"]!["items"]!.AsArray().Select(item => item!["offer_id"]!.GetValue<string>()));
        var lastOffer = await Execute(repository, "list_offers", new { sku = "SKU-001", limit = 2, cursor = offerCursor });
        Assert.Single(lastOffer["data"]!["items"]!.AsArray());
        Assert.Equal("quote-c", lastOffer["data"]!["items"]![0]!["offer_id"]!.GetValue<string>());
        Assert.Null(lastOffer["data"]!["next_cursor"]);
    }

    [Fact]
    public async Task FailedJournalWriteRollsBackDraftAndDoesNotReserveRequest()
    {
        await using var dataSource = DataSource();
        var eventId = await SeedNeed(dataSource);
        var repository = new PurchaseRepository(dataSource);
        var orderId = Guid.NewGuid().ToString("N");
        var request = Request("create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 10
        });
        var requestKey = PurchaseContract.IdentityKey(request.RequestId);
        await using (var block = dataSource.CreateCommand(
            $"ALTER TABLE purchase_requests ADD CONSTRAINT test_journal_failure CHECK (request_key <> '{requestKey}')"))
        {
            await block.ExecuteNonQueryAsync();
        }
        try
        {
            await Assert.ThrowsAsync<PostgresException>(() => repository.Process(request));
            await using var count = dataSource.CreateCommand(
                "SELECT count(*) FROM purchase_orders WHERE order_id = @id");
            count.Parameters.AddWithValue("id", orderId);
            Assert.Equal(0L, await count.ExecuteScalarAsync());
        }
        finally
        {
            await using var unblock = dataSource.CreateCommand(
                "ALTER TABLE purchase_requests DROP CONSTRAINT test_journal_failure");
            await unblock.ExecuteNonQueryAsync();
        }
        var result = JsonNode.Parse(await repository.Process(request))!;
        Assert.Equal("OK", result["outcome"]!.GetValue<string>());
    }

    [Fact]
    public async Task ConcurrentDifferentRequestsCannotReuseOrderId()
    {
        await using var dataSource = DataSource();
        var eventId = await SeedNeed(dataSource);
        var repository = new PurchaseRepository(dataSource);
        var orderId = Guid.NewGuid().ToString("N");
        var requests = new[] { 10, 20 }.Select(quantity => Request("create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity
        })).ToArray();
        var results = await Task.WhenAll(requests.Select(request => repository.Process(request)));
        Assert.Single(results, result => JsonNode.Parse(result)!["outcome"]!.GetValue<string>() == "OK");
        Assert.Single(results, result => JsonNode.Parse(result)!["outcome"]!.GetValue<string>() == "CONFLICT");
    }

    [Fact]
    public async Task LongRequestAndOrderIdsDoNotDependOnTextIndexLimits()
    {
        await using var dataSource = DataSource();
        var eventId = await SeedNeed(dataSource);
        var repository = new PurchaseRepository(dataSource);
        var orderId = new string('o', 10000) + Guid.NewGuid().ToString("N");
        var requestId = new string('r', 10000) + Guid.NewGuid().ToString("N");
        var request = Request("create_draft", new
        {
            order_id = orderId, need_event_id = eventId, offer_id = "quote-a", quantity = 10
        }, requestId);
        var result = await repository.Process(request);
        Assert.Equal(result, await repository.Process(request));
        var fetched = await Execute(repository, "get_order", new { order_id = orderId });
        Assert.Equal(orderId, fetched["data"]!["order_id"]!.GetValue<string>());
    }

    [Fact]
    public async Task IntegerValuedJsonNumbersAreAcceptedWithoutChangingMeaning()
    {
        await using var dataSource = DataSource();
        await new NeedRepository(dataSource).Migrate(CancellationToken.None);
        var repository = new PurchaseRepository(dataSource);
        var request = Request("list_offers", new { sku = "SKU-001", limit = 1, cursor = (string?)null });
        var json = request.Document.ToJsonString().Replace("\"limit\":1", "\"limit\":1e0");
        var parsed = PurchaseContract.Parse(request.RequestId, json);
        var result = JsonNode.Parse(await repository.Process(parsed))!;
        Assert.Single(result["data"]!["items"]!.AsArray());
    }
}
