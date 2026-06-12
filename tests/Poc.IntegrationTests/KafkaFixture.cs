using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.Kafka;
using Xunit;

namespace Poc.IntegrationTests;

/// <summary>
/// Spins up a single-node Kafka (KRaft) broker plus a Confluent Schema Registry on a shared Docker
/// network, once for the whole test run. Schema Registry reaches the broker over the in-network
/// BROKER listener (kafka:9093); tests reach both via host-mapped ports.
/// </summary>
public sealed class KafkaFixture : IAsyncLifetime
{
    private const string KafkaImage = "confluentinc/cp-kafka:7.7.1";
    private const string SchemaRegistryImage = "confluentinc/cp-schema-registry:7.7.1";

    private INetwork _network = null!;
    private KafkaContainer _kafka = null!;
    private IContainer _schemaRegistry = null!;

    public string BootstrapServers { get; private set; } = string.Empty;
    public string SchemaRegistryUrl { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().Build();
        await _network.CreateAsync();

        _kafka = new KafkaBuilder()
            .WithImage(KafkaImage)
            .WithNetwork(_network)
            .WithHostname("kafka")
            .WithNetworkAliases("kafka")
            .Build();
        await _kafka.StartAsync();

        _schemaRegistry = new ContainerBuilder()
            .WithImage(SchemaRegistryImage)
            .WithNetwork(_network)
            .WithPortBinding(8081, true)
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9093")
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8081).ForPath("/subjects")))
            .Build();
        await _schemaRegistry.StartAsync();

        BootstrapServers = StripScheme(_kafka.GetBootstrapAddress());
        SchemaRegistryUrl = $"http://{_schemaRegistry.Hostname}:{_schemaRegistry.GetMappedPublicPort(8081)}";
    }

    public async Task DisposeAsync()
    {
        if (_schemaRegistry is not null)
        {
            await _schemaRegistry.DisposeAsync();
        }

        if (_kafka is not null)
        {
            await _kafka.DisposeAsync();
        }

        if (_network is not null)
        {
            await _network.DeleteAsync();
        }
    }

    private static string StripScheme(string bootstrap) =>
        bootstrap.StartsWith("PLAINTEXT://", StringComparison.OrdinalIgnoreCase)
            ? bootstrap["PLAINTEXT://".Length..]
            : bootstrap;
}

[CollectionDefinition(Name)]
public sealed class KafkaCollection : ICollectionFixture<KafkaFixture>
{
    public const string Name = "kafka";
}
