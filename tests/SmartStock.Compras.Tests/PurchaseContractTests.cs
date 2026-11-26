using System.Text.Json.Nodes;
using Microsoft.Extensions.Configuration;
using SmartStock.Compras;
using Xunit;

namespace SmartStock.Compras.Tests;

public class PurchaseContractTests
{
    public static PurchaseRequest Request(string type, object payload, string? requestId = null)
    {
        var document = new JsonObject
        {
            ["request_id"] = requestId ?? Guid.NewGuid().ToString("N"),
            ["type"] = type,
            ["version"] = 1,
            ["occurred_at"] = "2026-11-25T00:00:00Z",
            ["payload"] = System.Text.Json.JsonSerializer.SerializeToNode(payload)
        };
        return PurchaseContract.Parse(document["request_id"]!.GetValue<string>(), document.ToJsonString());
    }

    [Theory]
    [InlineData("get_need", """{"event_id":"evt"}""")]
    [InlineData("list_needs", """{"sku":"SKU-001","limit":1,"cursor":null}""")]
    [InlineData("list_offers", """{"sku":"SKU-001","limit":100,"cursor":null}""")]
    [InlineData("get_order", """{"order_id":"order"}""")]
    [InlineData("create_draft", """{"order_id":"order","need_event_id":"evt","offer_id":"quote-a","quantity":10}""")]
    [InlineData("decide_order", """{"order_id":"order","expected_revision":1,"decision":"APPROVE","actor_id":"operator"}""")]
    public void AcceptsAllSixOperations(string type, string payload)
    {
        var request = Request(type, JsonNode.Parse(payload)!);
        Assert.Equal(type, request.Type);
    }

    [Theory]
    [InlineData("get_need", """{"event_id":" "}""")]
    [InlineData("get_need", """{"event_id":"evt","unknown":true}""")]
    [InlineData("list_needs", """{"sku":"SKU-001","limit":0,"cursor":null}""")]
    [InlineData("list_offers", """{"sku":"SKU-001","limit":101,"cursor":null}""")]
    [InlineData("list_offers", """{"sku":"SKU-001","limit":1.5,"cursor":null}""")]
    [InlineData("list_offers", """{"sku":"SKU-001","limit":1}""")]
    [InlineData("get_order", """{"order_id":12}""")]
    [InlineData("create_draft", """{"order_id":"o","need_event_id":"e","offer_id":"q","quantity":2147483648}""")]
    [InlineData("decide_order", """{"order_id":"o","expected_revision":1,"decision":"APPROVED","actor_id":"operator"}""")]
    [InlineData("decide_order", """{"order_id":"o","expected_revision":0,"decision":"APPROVE","actor_id":"operator"}""")]
    public void RejectsInvalidPayloads(string type, string payload)
    {
        Assert.Throws<FormatException>(() => Request(type, JsonNode.Parse(payload)!));
    }

    [Theory]
    [InlineData("not-a-dateZ")]
    [InlineData("2026-11-25T00:00:00+00:00")]
    [InlineData("2026-13-25T00:00:00Z")]
    [InlineData("2026-02-29T00:00:00Z")]
    [InlineData("2026-04-31T00:00:00Z")]
    [InlineData("2026-11-25T24:00:00Z")]
    [InlineData("2026-11-25T12:00:60Z")]
    public void RejectsInvalidTimestamp(string timestamp)
    {
        var request = Request("get_need", new { event_id = "evt" });
        request.Document["occurred_at"] = timestamp;
        Assert.Throws<FormatException>(() => PurchaseContract.Parse(request.RequestId, request.Document.ToJsonString()));
    }

    [Fact]
    public void RejectsMalformedEnvelopeAndKey()
    {
        var request = Request("get_need", new { event_id = "evt" });
        Assert.Throws<FormatException>(() => PurchaseContract.Parse("wrong-key", request.Document.ToJsonString()));
        Assert.Throws<FormatException>(() => PurchaseContract.Parse(null, request.Document.ToJsonString()));
        Assert.Throws<FormatException>(() => PurchaseContract.Parse(request.RequestId, null));
        Assert.Throws<FormatException>(() => PurchaseContract.Parse("id", "{"));
        Assert.Throws<FormatException>(() => PurchaseContract.Parse("id",
            """{"request_id":"id","type":"get_need","version":1,"occurred_at":"2026-11-25T00:00:00Z","payload":{"event_id":"a","event_id":"b"}}"""));
        request.Document["unknown"] = true;
        Assert.Throws<FormatException>(() => PurchaseContract.Parse(request.RequestId, request.Document.ToJsonString()));
    }

    [Theory]
    [InlineData(null, null, false)]
    [InlineData(null, "true", false)]
    [InlineData("production", "true", false)]
    [InlineData("local", null, false)]
    [InlineData("local", "false", false)]
    [InlineData("local", "true", true)]
    public void DecisionsRequireBothLocalEnvironmentAndExplicitOptIn(string? environment, string? flag, bool enabled)
    {
        var policy = Policy(environment, flag);
        Assert.Equal(enabled, policy.CanDecide("local-operator"));
        Assert.False(policy.CanDecide("other-operator"));
    }

    [Fact]
    public void DecisionActorIsExactAndConfigured()
    {
        var policy = Policy("local", "true", "CaseSensitiveActor");
        Assert.True(policy.CanDecide("CaseSensitiveActor"));
        Assert.False(policy.CanDecide("casesensitiveactor"));
        Assert.False(policy.CanDecide("local-operator"));
        Assert.False(Policy("local", "true", " ").Enabled);
    }

    [Fact]
    public void RejectsInvalidOutgoingData()
    {
        var request = Request("get_need", new { event_id = "evt" });
        Assert.Throws<FormatException>(() => PurchaseContract.CreateResult(request, "OK", null));
        Assert.Throws<FormatException>(() => PurchaseContract.CreateResult(request, "NOT_FOUND", new JsonObject()));
    }

    public static PurchaseDecisionPolicy Policy(string? environment = "local", string? flag = "true", string? actor = null)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["PURCHASES_ENVIRONMENT"] = environment,
            ["PURCHASES_ALLOW_LOCAL_DECISIONS"] = flag,
            ["PURCHASES_LOCAL_ACTOR_ID"] = actor
        }).Build();
        return PurchaseDecisionPolicy.FromConfiguration(configuration);
    }
}
