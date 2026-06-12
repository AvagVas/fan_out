using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Poc.Contracts;
using Poc.Kafka;

namespace Poc.Enricher.Streams;

/// <summary>
/// Manual equivalent of a Kafka Streams <c>KTable(proxy.requests) ⋈ KTable(service-a.completed)</c>
/// inner join on <c>correlationId</c>, emitting <see cref="ServiceBCommand"/> to <c>service-b.ready</c>.
///
/// Why manual (not Streamiz): chosen to retain full control over the join's idempotency ledger and
/// the required pending/joined/duplicate/failed metrics, which a black-box DSL join does not expose.
///
/// Semantics:
///  - Each input topic is materialized as a durable latest-value-per-key table (the KTable).
///  - On every record, the opposite table is looked up by key. When both sides exist the join fires
///    exactly once — guarded by the <c>emitted</c> ledger — regardless of arrival order.
///  - Output is produced with an idempotent producer and the ack is awaited before the SQLite
///    transaction (state upsert + emitted mark) commits, then the Kafka offset is committed.
///  - Restart-safe: state + emitted ledger live in SQLite; re-consumed records cannot re-emit.
/// </summary>
public sealed class EnricherProcessor : IDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly EnricherStateStore _state;
    private readonly EnricherMetrics _metrics;
    private readonly ILogger<EnricherProcessor> _logger;
    private readonly KafkaTopics _topics;

    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IProducer<string, ServiceBCommand> _producer;
    private readonly IAsyncDeserializer<RequestReceived> _requestDeserializer;
    private readonly IAsyncDeserializer<ServiceACompleted> _completionDeserializer;

    public EnricherProcessor(
        KafkaClientFactory kafka,
        EnricherStateStore state,
        EnricherMetrics metrics,
        ILogger<EnricherProcessor> logger,
        string groupId = "enricher")
    {
        _state = state;
        _metrics = metrics;
        _logger = logger;
        _topics = kafka.Options.Topics;

        _consumer = kafka.CreateRawConsumer(groupId);
        _producer = kafka.CreateProducer<ServiceBCommand>("enricher");
        _requestDeserializer = kafka.CreateValueDeserializer<RequestReceived>();
        _completionDeserializer = kafka.CreateValueDeserializer<ServiceACompleted>();
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(new[] { _topics.ProxyRequests, _topics.ServiceACompleted });
        _logger.LogInformation("Enricher subscribed to {Requests} and {Completions}",
            _topics.ProxyRequests, _topics.ServiceACompleted);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> result;
                try
                {
                    result = _consumer.Consume(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (result is null || result.IsPartitionEOF)
                {
                    continue;
                }

                try
                {
                    await HandleAsync(result, cancellationToken);
                    _consumer.Commit(result); // commit only after the join is durably recorded
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _metrics.RecordFailed();
                    _logger.LogError(ex, "Failed to process {Topic}@{Partition}:{Offset}; will retry",
                        result.Topic, result.Partition.Value, result.Offset.Value);
                    SeekBack(result);
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        try { await Task.Delay(500, cancellationToken); } catch (OperationCanceledException) { break; }
                    }
                }
            }
        }
        finally
        {
            _consumer.Close(); // graceful shutdown: leave the group and flush final offsets
        }
    }

    private async Task HandleAsync(ConsumeResult<string, byte[]> result, CancellationToken cancellationToken)
    {
        var key = result.Message.Key;
        using var scope = LogScope.Correlation(_logger, key);

        var isRequest = result.Topic == _topics.ProxyRequests;
        var context = new SerializationContext(MessageComponentType.Value, result.Topic, result.Message.Headers);
        var isNull = result.Message.Value is null || result.Message.Value.Length == 0;

        // Materialize the incoming side as canonical JSON for the state store.
        string incomingJson = isRequest
            ? JsonSerializer.Serialize(await _requestDeserializer.DeserializeAsync(result.Message.Value, isNull, context), Json)
            : JsonSerializer.Serialize(await _completionDeserializer.DeserializeAsync(result.Message.Value, isNull, context), Json);

        using var txn = _state.BeginTransaction();

        if (isRequest)
        {
            _state.UpsertRequest(key, incomingJson, txn);
        }
        else
        {
            _state.UpsertCompletion(key, incomingJson, txn);
        }

        var requestJson = _state.TryGetRequest(key, txn);
        var completionJson = _state.TryGetCompletion(key, txn);

        // Only one side present yet → KTable inner join emits nothing.
        if (requestJson is null || completionJson is null)
        {
            txn.Commit();
            _metrics.RecordUnmatched();
            _logger.LogInformation("Unmatched {Side} for {Key}: waiting for counterpart",
                isRequest ? "request" : "completion", key);
            return;
        }

        // Both sides present but already emitted → idempotency guard blocks a duplicate output.
        if (_state.IsEmitted(key, txn))
        {
            txn.Commit();
            _metrics.RecordDuplicate();
            _logger.LogInformation("Duplicate input for {Key}: join already emitted, skipping", key);
            return;
        }

        var request = JsonSerializer.Deserialize<RequestReceived>(requestJson, Json)!;
        var completion = JsonSerializer.Deserialize<ServiceACompleted>(completionJson, Json)!;
        var command = new ServiceBCommand
        {
            CorrelationId = key,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            OriginalPayload = request.Payload,
            ServiceAIds = completion.ServiceAIds,
        };

        await _producer.ProduceAsync(_topics.ServiceBReady, new Message<string, ServiceBCommand>
        {
            Key = key,
            Value = command,
            Headers = new Headers { { KafkaHeaders.CorrelationId, Encoding.UTF8.GetBytes(key) } },
        }, cancellationToken);

        _state.MarkEmitted(key, DateTimeOffset.UtcNow, txn);
        txn.Commit();

        _metrics.RecordJoined();
        _logger.LogInformation("Joined request + completion for {Key}; emitted ServiceBCommand to {Topic}",
            key, _topics.ServiceBReady);
    }

    private void SeekBack(ConsumeResult<string, byte[]> result)
    {
        try
        {
            _consumer.Seek(result.TopicPartitionOffset);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Seek-back failed for {Topic}@{Partition}", result.Topic, result.Partition.Value);
        }
    }

    public void Dispose()
    {
        _consumer.Dispose();
        _producer.Dispose();
        (_requestDeserializer as IDisposable)?.Dispose();
        (_completionDeserializer as IDisposable)?.Dispose();
    }
}
