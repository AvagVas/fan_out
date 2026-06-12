using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Options;
using NJsonSchema.NewtonsoftJson.Generation;
using Newtonsoft.Json.Serialization;

namespace Poc.Kafka;

/// <summary>
/// Builds idempotent producers, manually-committing consumers, and admin clients pre-wired with
/// JSON-Schema-backed serdes against the Confluent Schema Registry. One place owns all client config
/// (idempotence, security, offset strategy) so every service behaves consistently.
/// </summary>
public sealed class KafkaClientFactory
{
    private readonly KafkaOptions _options;
    private readonly ISchemaRegistryClient _schemaRegistry;

    // camelCase on the wire to match the documented JSON contracts and render cleanly in Kafka UI.
    private static readonly NewtonsoftJsonSchemaGeneratorSettings JsonSettings = new()
    {
        SerializerSettings = new Newtonsoft.Json.JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
        },
    };

    public KafkaClientFactory(IOptions<KafkaOptions> options, ISchemaRegistryClient schemaRegistry)
    {
        _options = options.Value;
        _schemaRegistry = schemaRegistry;
    }

    public KafkaOptions Options => _options;

    /// <summary>Idempotent producer (exactly-once-per-producer-session semantics on retries).</summary>
    public IProducer<string, T> CreateProducer<T>(string clientId) where T : class
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = clientId,
            EnableIdempotence = true,          // dedupes producer retries, guarantees ordering
            Acks = Acks.All,                   // wait for all in-sync replicas
            MessageSendMaxRetries = 10,
            MaxInFlight = 5,                   // max allowed with idempotence enabled
            LingerMs = 5,
        };
        ApplySecurity(config);

        var serializerConfig = new JsonSerializerConfig { AutoRegisterSchemas = true };
        return new ProducerBuilder<string, T>(config)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(new JsonSerializer<T>(_schemaRegistry, serializerConfig, JsonSettings))
            .Build();
    }

    /// <summary>Consumer with auto-commit disabled — callers commit only after successful processing.</summary>
    public IConsumer<string, T> CreateConsumer<T>(string groupId) where T : class
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        ApplySecurity(config);

        return new ConsumerBuilder<string, T>(config)
            .SetKeyDeserializer(Deserializers.Utf8)
            // Same camelCase settings as the serializer: the deserializer validates against a schema
            // generated from these settings, so they must match the wire format.
            .SetValueDeserializer(new JsonDeserializer<T>(_schemaRegistry, jsonSchemaGeneratorSettings: JsonSettings).AsSyncOverAsync())
            .Build();
    }

    /// <summary>
    /// Raw byte consumer for callers (the Enricher) that subscribe to multiple topics carrying
    /// different value types and deserialize per-topic with <see cref="CreateValueDeserializer{T}"/>.
    /// </summary>
    public IConsumer<string, byte[]> CreateRawConsumer(string groupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        ApplySecurity(config);

        return new ConsumerBuilder<string, byte[]>(config)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(Deserializers.ByteArray)
            .Build();
    }

    /// <summary>Schema-Registry-aware JSON deserializer for a single message type.</summary>
    public IAsyncDeserializer<T> CreateValueDeserializer<T>() where T : class =>
        new JsonDeserializer<T>(_schemaRegistry, jsonSchemaGeneratorSettings: JsonSettings);

    public IAdminClient CreateAdminClient()
    {
        var config = new AdminClientConfig { BootstrapServers = _options.BootstrapServers };
        ApplySecurity(config);
        return new AdminClientBuilder(config).Build();
    }

    private void ApplySecurity(ClientConfig config)
    {
        if (!_options.UseSasl)
        {
            return;
        }

        config.SecurityProtocol = SecurityProtocol.SaslSsl;
        config.SaslMechanism = SaslMechanism.Plain;
        config.SaslUsername = _options.SaslUsername;
        config.SaslPassword = _options.SaslPassword;
    }
}