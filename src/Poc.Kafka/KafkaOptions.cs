namespace Poc.Kafka;

/// <summary>
/// Strongly typed Kafka configuration bound from the <c>Kafka</c> config section, with flat
/// environment variables (Confluent Cloud style) applied as overrides on top.
/// </summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string SchemaRegistryUrl { get; set; } = "http://localhost:8081";
    public KafkaTopics Topics { get; set; } = new();

    // Confluent Cloud / secured cluster credentials — normally supplied via environment.
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public string? SchemaRegistryApiKey { get; set; }
    public string? SchemaRegistryApiSecret { get; set; }

    public bool UseSasl => !string.IsNullOrWhiteSpace(SaslUsername) && !string.IsNullOrWhiteSpace(SaslPassword);

    /// <summary>
    /// Applies the spec-mandated flat environment variables (BOOTSTRAP_SERVERS, SASL_USERNAME, ...)
    /// over whatever was bound from appsettings, so the same image works locally and on Confluent Cloud.
    /// </summary>
    public void ApplyEnvironmentOverrides()
    {
        BootstrapServers = Env("BOOTSTRAP_SERVERS") ?? BootstrapServers;
        SchemaRegistryUrl = Env("SCHEMA_REGISTRY_URL") ?? SchemaRegistryUrl;
        SaslUsername = Env("SASL_USERNAME") ?? SaslUsername;
        SaslPassword = Env("SASL_PASSWORD") ?? SaslPassword;
        SchemaRegistryApiKey = Env("SCHEMA_REGISTRY_API_KEY") ?? SchemaRegistryApiKey;
        SchemaRegistryApiSecret = Env("SCHEMA_REGISTRY_API_SECRET") ?? SchemaRegistryApiSecret;
    }

    private static string? Env(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}

public sealed class KafkaTopics
{
    public string ProxyRequests { get; set; } = "proxy.requests";
    public string ServiceACompleted { get; set; } = "service-a.completed";
    public string ServiceBReady { get; set; } = "service-b.ready";
    public string ServiceBDlq { get; set; } = "service-b.dlq";
}
