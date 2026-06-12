using Confluent.Kafka;
using Poc.Contracts;
using Poc.Kafka;

namespace Poc.ServiceA.Api;

/// <summary>
/// Publishing seam for Service A's completion event. The PoC publishes directly to Kafka, but the
/// interface is deliberately the only way the handler emits events so a <b>Transactional Outbox</b>
/// can be dropped in later: the implementation would instead INSERT the event into an outbox table
/// inside the same DB transaction as the business write, and a relay/CDC process would publish it.
/// </summary>
public interface IOutbox
{
    Task PublishAsync(ServiceACompleted message, CancellationToken cancellationToken);
}

/// <summary>PoC implementation: idempotent direct publish to <c>service-a.completed</c>.</summary>
public sealed class DirectKafkaOutbox : IOutbox
{
    private readonly IProducer<string, ServiceACompleted> _producer;
    private readonly KafkaClientFactory _kafka;
    private readonly ILogger<DirectKafkaOutbox> _logger;

    public DirectKafkaOutbox(IProducer<string, ServiceACompleted> producer, KafkaClientFactory kafka, ILogger<DirectKafkaOutbox> logger)
    {
        _producer = producer;
        _kafka = kafka;
        _logger = logger;
    }

    public async Task PublishAsync(ServiceACompleted message, CancellationToken cancellationToken)
    {
        var delivery = await _producer.ProduceAsync(
            _kafka.Options.Topics.ServiceACompleted,
            new Message<string, ServiceACompleted>
            {
                Key = message.CorrelationId, // key = correlationId, same as the originating request
                Value = message,
                Headers = new Headers { { KafkaHeaders.CorrelationId, System.Text.Encoding.UTF8.GetBytes(message.CorrelationId) } },
            },
            cancellationToken);

        _logger.LogInformation("Published ServiceACompleted to {Topic} at offset {Offset}", delivery.Topic, delivery.Offset.Value);
    }
}
