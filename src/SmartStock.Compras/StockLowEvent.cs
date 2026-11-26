using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace SmartStock.Compras;

public sealed record StockLowEvent(
    string EventId, string EventType, int EventVersion, DateTimeOffset OccurredAt,
    string Sku, int PreviousStock, int CurrentStock, int Threshold, string SaleId)
{
    private static readonly HashSet<string> Fields =
    [
        "event_id", "event_type", "event_version", "occurred_at", "sku",
        "previous_stock", "current_stock", "threshold", "sale_id"
    ];

    public static StockLowEvent Parse(string json)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                throw new FormatException("El evento debe ser un objeto JSON.");

            var names = new HashSet<string>();
            foreach (var property in root.EnumerateObject())
            {
                if (!Fields.Contains(property.Name) || !names.Add(property.Name))
                    throw new FormatException($"Campo desconocido o duplicado: {property.Name}.");
            }

            if (!names.SetEquals(Fields))
                throw new FormatException("Faltan campos obligatorios.");

            string Text(string name)
            {
                var value = root.GetProperty(name);
                if (value.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(value.GetString()))
                    throw new FormatException($"Campo inválido: {name}.");
                return value.GetString()!;
            }

            int Number(string name)
            {
                var value = root.GetProperty(name);
                if (value.ValueKind != JsonValueKind.Number || !value.TryGetDecimal(out var number) ||
                    number < 0 || number > int.MaxValue || decimal.Truncate(number) != number)
                    throw new FormatException($"Campo inválido: {name}.");
                return (int)number;
            }

            var eventType = Text("event_type");
            var version = Number("event_version");
            var timestamp = Text("occurred_at");
            if (eventType != "inventory.stock_low" || version != 1 ||
                !Regex.IsMatch(timestamp, @"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$") ||
                !DateTimeOffset.TryParse(timestamp, CultureInfo.InvariantCulture, DateTimeStyles.None, out var occurredAt))
                throw new FormatException("Tipo, versión o fecha del evento inválidos.");

            var previous = Number("previous_stock");
            var current = Number("current_stock");
            var threshold = Number("threshold");
            if (previous <= threshold || current > threshold)
                throw new FormatException("El evento no representa un cruce del umbral.");

            return new StockLowEvent(Text("event_id"), eventType, version, occurredAt,
                Text("sku"), previous, current, threshold, Text("sale_id"));
        }
        catch (JsonException exception)
        {
            throw new FormatException("JSON inválido.", exception);
        }
    }
}
