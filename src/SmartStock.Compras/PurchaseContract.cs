using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Json.Schema;

namespace SmartStock.Compras;

public sealed record PurchaseRequest(string RequestId, string Type, JsonObject Document)
{
    public JsonObject Payload => (JsonObject)Document["payload"]!;
}

public static class PurchaseContract
{
    private static readonly JsonSchema RequestSchema = Load("Purchases.Requests.schema.json");
    private static readonly JsonSchema ResultSchema = Load("Purchases.Results.schema.json");
    private static readonly JsonSchema OfferSchema = LoadOfferSchema();
    private static readonly Regex UtcTimestamp = new(
        @"\A(?<year>[0-9]{4})-(?<month>0[1-9]|1[0-2])-(?<day>0[1-9]|[12][0-9]|3[01])[Tt](?<hour>[01][0-9]|2[0-3]):(?<minute>[0-5][0-9]):(?<second>[0-5][0-9]|60)(\.[0-9]+)?Z\z",
        RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);

    public static PurchaseRequest Parse(string? key, string? json)
    {
        try
        {
            if (json is null)
                throw new FormatException("La solicitud no puede ser nula.");

            // Duplicate property names have no interoperable JSON meaning.
            using var document = JsonDocument.Parse(json);
            RejectDuplicateProperties(document.RootElement);
            var node = JsonNode.Parse(json);
            Validate(RequestSchema, node);
            var request = (JsonObject)node!;
            var requestId = request["request_id"]!.GetValue<string>();
            if (!string.Equals(key, requestId, StringComparison.Ordinal))
                throw new FormatException("La clave Kafka no coincide con request_id.");

            return new PurchaseRequest(requestId, request["type"]!.GetValue<string>(), request);
        }
        catch (JsonException exception)
        {
            throw new FormatException("JSON de solicitud inválido.", exception);
        }
    }

    public static string CreateResult(PurchaseRequest request, string outcome, JsonNode? data)
    {
        var result = new JsonObject
        {
            ["request_id"] = request.RequestId,
            ["type"] = request.Type,
            ["version"] = 1,
            ["occurred_at"] = DateTime.UtcNow.ToString("O"),
            ["outcome"] = outcome,
            ["data"] = data
        };
        ValidateResult(result);
        return result.ToJsonString();
    }

    public static void ValidateResult(JsonNode? result) => Validate(ResultSchema, result);

    public static bool IsValidOffer(JsonObject offer) => OfferSchema.Evaluate(offer).IsValid;

    public static string IdentityKey(string id) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(id)));

    private static JsonSchema Load(string name)
    {
        using var stream = typeof(PurchaseContract).Assembly.GetManifestResourceStream(name)
            ?? throw new InvalidOperationException($"No se encontró el contrato {name}.");
        using var reader = new StreamReader(stream);
        return JsonSchema.FromText(reader.ReadToEnd());
    }

    private static JsonSchema LoadOfferSchema()
    {
        using var stream = typeof(PurchaseContract).Assembly.GetManifestResourceStream("Purchases.Results.schema.json")!;
        using var reader = new StreamReader(stream);
        var resultSchema = JsonNode.Parse(reader.ReadToEnd())!;
        var offerSchema = new JsonObject
        {
            ["$schema"] = "https://json-schema.org/draft/2020-12/schema",
            ["$ref"] = "#/$defs/offer",
            ["$defs"] = resultSchema["$defs"]!.DeepClone()
        };
        return JsonSchema.FromText(offerSchema.ToJsonString());
    }

    private static void Validate(JsonSchema schema, JsonNode? node)
    {
        var result = schema.Evaluate(node, new EvaluationOptions
        {
            RequireFormatValidation = true,
            OutputFormat = OutputFormat.Flag
        });
        if (!result.IsValid)
            throw new FormatException("El mensaje no cumple el contrato Kafka de Compras.");

        ValidateTimestamp(node!["occurred_at"]!.GetValue<string>());
    }

    private static void ValidateTimestamp(string timestamp)
    {
        // Supplement the library's permissive date-time format with calendar validation,
        // without rounding fractional seconds or converting the preserved timestamp.
        var match = UtcTimestamp.Match(timestamp);
        if (!match.Success)
            throw new FormatException("occurred_at debe ser una fecha-hora RFC 3339 UTC terminada en Z.");
        var year = int.Parse(match.Groups["year"].Value, System.Globalization.CultureInfo.InvariantCulture);
        var month = int.Parse(match.Groups["month"].Value, System.Globalization.CultureInfo.InvariantCulture);
        var day = int.Parse(match.Groups["day"].Value, System.Globalization.CultureInfo.InvariantCulture);
        var daysInMonth = DateTime.DaysInMonth(year == 0 ? 400 : year, month);
        var validLeapSecond = match.Groups["second"].Value != "60"
            || (match.Groups["hour"].Value == "23" && match.Groups["minute"].Value == "59");
        if (day > daysInMonth || !validLeapSecond)
            throw new FormatException("occurred_at contiene una fecha-hora inválida.");
    }

    private static void RejectDuplicateProperties(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in element.EnumerateObject())
            {
                if (!names.Add(property.Name))
                    throw new FormatException("El mensaje contiene campos JSON duplicados.");
                RejectDuplicateProperties(property.Value);
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in element.EnumerateArray())
                RejectDuplicateProperties(item);
        }
    }
}

public sealed class PurchaseRequestConflictException(string message) : Exception(message);
