using SmartStock.Compras;
using Xunit;

namespace SmartStock.Compras.Tests;

public class StockLowEventTests
{
    public const string Example = """
        {"event_id":"evt-sale-001","event_type":"inventory.stock_low","event_version":1,
         "occurred_at":"2026-01-01T12:00:00Z","sku":"SKU-001","previous_stock":10,
         "current_stock":7,"threshold":7,"sale_id":"sale-001"}
        """;

    [Fact]
    public void ParsesThresholdCrossing()
    {
        var result = StockLowEvent.Parse(Example);
        Assert.Equal("evt-sale-001", result.EventId);
        Assert.Equal(10, result.PreviousStock);
        Assert.Equal(7, result.CurrentStock);
    }

    [Theory]
    [InlineData("\"event_version\":1", "\"event_version\":2")]
    [InlineData("\"event_type\":\"inventory.stock_low\"", "\"event_type\":\"other\"")]
    [InlineData("\"previous_stock\":10", "\"previous_stock\":7")]
    [InlineData("\"current_stock\":7", "\"current_stock\":8")]
    [InlineData("\"threshold\":7", "\"threshold\":-1")]
    [InlineData("\"sku\":\"SKU-001\"", "\"sku\":\" \"")]
    [InlineData("\"sale_id\":\"sale-001\"", "\"sale_id\":null")]
    [InlineData("2026-01-01T12:00:00Z", "2026-01-01T12:00:00+00:00")]
    public void RejectsInvalidEvents(string original, string replacement)
    {
        Assert.Throws<FormatException>(() => StockLowEvent.Parse(Example.Replace(original, replacement)));
    }

    [Fact]
    public void RejectsUnexpectedFields()
    {
        Assert.Throws<FormatException>(() => StockLowEvent.Parse(Example.Replace("{", "{\"extra\":1,")));
    }

    [Fact]
    public void RejectsMalformedJson()
    {
        Assert.Throws<FormatException>(() => StockLowEvent.Parse("{"));
    }
}
