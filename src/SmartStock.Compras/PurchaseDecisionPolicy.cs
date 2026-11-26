namespace SmartStock.Compras;

public sealed class PurchaseDecisionPolicy
{
    private readonly string actorId;
    public bool Enabled { get; }

    private PurchaseDecisionPolicy(bool enabled, string actorId)
    {
        Enabled = enabled;
        this.actorId = actorId;
    }

    public static PurchaseDecisionPolicy FromConfiguration(IConfiguration configuration)
    {
        var localEnvironment = string.Equals(
            configuration["PURCHASES_ENVIRONMENT"], "local", StringComparison.Ordinal);
        var explicitOptIn = string.Equals(
            configuration["PURCHASES_ALLOW_LOCAL_DECISIONS"], "true", StringComparison.OrdinalIgnoreCase);
        var actorId = configuration["PURCHASES_LOCAL_ACTOR_ID"] ?? "local-operator";
        return new PurchaseDecisionPolicy(localEnvironment && explicitOptIn && !string.IsNullOrWhiteSpace(actorId), actorId);
    }

    public bool CanDecide(string candidateActorId) =>
        Enabled && string.Equals(actorId, candidateActorId, StringComparison.Ordinal);
}
