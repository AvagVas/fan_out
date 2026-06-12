using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Poc.Contracts;
using Poc.Enricher.Streams;
using Poc.Kafka;

namespace Poc.IntegrationTests;

/// <summary>
/// Per-test isolation: unique topic names + a fresh SQLite state path on top of the shared broker, so
/// tests never interfere. Provides produce/consume helpers and an enricher runner.
/// </summary>
public sealed class PipelineHarness : IAsyncDisposable
{
    public KafkaOptions Options { get; }
    public KafkaClientFactory Kafka { get; }
    public string StatePath { get; }
    public string Suffix { get; }

    private readonly ISchemaRegistryClient _schemaRegistry;

    private PipelineHarness(KafkaOptions options, KafkaClientFactory kafka, ISchemaRegistryClient sr, string statePath, string suffix)
    {
        Options = options;
        Kafka = kafka;
        _schemaRegistry = sr;
        StatePath = statePath;
        Suffix = suffix;
    }

    public static async Task<PipelineHarness> CreateAsync(KafkaFixture fixture)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var options = new KafkaOptions
        {
            BootstrapServers = fixture.BootstrapServers,
            SchemaRegistryUrl = fixture.SchemaRegistryUrl,
            Topics = new KafkaTopics
            {
                ProxyRequests = $"proxy.requests.{suffix}",
                ServiceACompleted = $"service-a.completed.{suffix}",
                ServiceBReady = $"service-b.ready.{suffix}",
                ServiceBDlq = $"service-b.dlq.{suffix}",
            },
        };

        var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = options.SchemaRegistryUrl });
        var factory = new KafkaClientFactory(Microsoft.Extensions.Options.Options.Create(options), schemaRegistry);

        var provisioner = new TopicProvisioner(factory, NullLogger<TopicProvisioner>.Instance);
        await provisioner.EnsureTopicsAsync(partitions: 1, replicationFactor: 1);

        var statePath = Path.Combine(Path.GetTempPath(), $"enricher-{suffix}.db");
        return new PipelineHarness(options, factory, schemaRegistry, statePath, suffix);
    }

    public async Task ProduceRequestAsync(RequestReceived request)
    {
        using var producer = Kafka.CreateProducer<RequestReceived>($"test-req-{Suffix}");
        await producer.ProduceAsync(Options.Topics.ProxyRequests, Keyed(request.CorrelationId, request));
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task ProduceCompletionAsync(ServiceACompleted completion)
    {
        using var producer = Kafka.CreateProducer<ServiceACompleted>($"test-comp-{Suffix}");
        await producer.ProduceAsync(Options.Topics.ServiceACompleted, Keyed(completion.CorrelationId, completion));
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task ProduceReadyAsync(ServiceBCommand command)
    {
        using var producer = Kafka.CreateProducer<ServiceBCommand>($"test-ready-{Suffix}");
        await producer.ProduceAsync(Options.Topics.ServiceBReady, Keyed(command.CorrelationId, command));
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    /// <summary>Consumes a single value from a topic within the timeout, or null if none arrives.</summary>
    public T? ConsumeOne<T>(string topic, TimeSpan timeout) where T : class
    {
        using var consumer = Kafka.CreateConsumer<T>($"reader-{topic}-{Guid.NewGuid():N}");
        consumer.Subscribe(topic);
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (result is not null && !result.IsPartitionEOF)
            {
                return result.Message.Value;
            }
        }

        return null;
    }

    /// <summary>Counts how many values land on a topic within the window (used to detect duplicates).</summary>
    public int CountWithin<T>(string topic, TimeSpan window) where T : class
    {
        using var consumer = Kafka.CreateConsumer<T>($"counter-{topic}-{Guid.NewGuid():N}");
        consumer.Subscribe(topic);
        var deadline = DateTime.UtcNow + window;
        var count = 0;
        while (DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (result is not null && !result.IsPartitionEOF)
            {
                count++;
            }
        }

        return count;
    }

    public EnricherRunner StartEnricher(string? groupId = null) =>
        EnricherRunner.Start(Kafka, StatePath, groupId ?? $"enricher-{Suffix}");

    private static Message<string, T> Keyed<T>(string key, T value) => new()
    {
        Key = key,
        Value = value,
        Headers = new Headers { { KafkaHeaders.CorrelationId, System.Text.Encoding.UTF8.GetBytes(key) } },
    };

    public ValueTask DisposeAsync()
    {
        _schemaRegistry.Dispose();
        try { File.Delete(StatePath); } catch { /* best effort */ }
        return ValueTask.CompletedTask;
    }
}

/// <summary>Runs an <see cref="EnricherProcessor"/> in the background; restart = stop + Start again on the same state path.</summary>
public sealed class EnricherRunner : IAsyncDisposable
{
    private readonly EnricherProcessor _processor;
    private readonly CancellationTokenSource _cts;
    private readonly Task _task;

    public EnricherStateStore State { get; }
    public EnricherMetrics Metrics { get; }

    private EnricherRunner(EnricherProcessor processor, EnricherStateStore state, EnricherMetrics metrics, CancellationTokenSource cts, Task task)
    {
        _processor = processor;
        State = state;
        Metrics = metrics;
        _cts = cts;
        _task = task;
    }

    public static EnricherRunner Start(KafkaClientFactory kafka, string statePath, string groupId)
    {
        var state = new EnricherStateStore(statePath);
        var metrics = new EnricherMetrics(state);
        var processor = new EnricherProcessor(kafka, state, metrics, NullLogger<EnricherProcessor>.Instance, groupId);
        var cts = new CancellationTokenSource();
        var task = Task.Run(() => processor.RunAsync(cts.Token));
        return new EnricherRunner(processor, state, metrics, cts, task);
    }

    /// <summary>Waits until the enricher has materialized the expected number of requests + completions.</summary>
    public async Task WaitForMaterializedAsync(long requests, long completions, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (State.CountRequests() >= requests && State.CountCompletions() >= completions)
            {
                return;
            }

            await Task.Delay(100);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _task; } catch { /* expected on cancel */ }
        _processor.Dispose();
        Metrics.Dispose();
        State.Dispose();
        _cts.Dispose();
    }
}
