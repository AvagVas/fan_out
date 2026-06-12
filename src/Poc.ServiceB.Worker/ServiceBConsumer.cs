using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Poc.Contracts;
using Poc.Kafka;

namespace Poc.ServiceB.Worker;

public sealed class ServiceBOptions
{
    public const string SectionName = "ServiceB";
    public string GroupId { get; set; } = "service-b";
    public int MaxAttempts { get; set; } = 3;
    public int RetryDelayMs { get; set; } = 200;
}

/// <summary>
/// Consumes <b>only</b> <c>service-b.ready</c> (never the raw <c>proxy.requests</c>), processes each
/// enriched command with a bounded retry policy, commits offsets only after success, and routes
/// poison messages to <c>service-b.dlq</c> so a single bad record never blocks the partition.
/// </summary>
public sealed class ServiceBConsumer : IDisposable
{
    private readonly IConsumer<string, ServiceBCommand> _consumer;
    private readonly IProducer<string, ServiceBCommand> _dlqProducer;
    private readonly IServiceBHandler _handler;
    private readonly ServiceBMetrics _metrics;
    private readonly ILogger<ServiceBConsumer> _logger;
    private readonly KafkaTopics _topics;
    private readonly ServiceBOptions _options;

    public ServiceBConsumer(
        KafkaClientFactory kafka,
        IServiceBHandler handler,
        ServiceBMetrics metrics,
        ILogger<ServiceBConsumer> logger,
        ServiceBOptions options)
    {
        _handler = handler;
        _metrics = metrics;
        _logger = logger;
        _options = options;
        _topics = kafka.Options.Topics;
        _consumer = kafka.CreateConsumer<ServiceBCommand>(options.GroupId);
        _dlqProducer = kafka.CreateProducer<ServiceBCommand>("service-b-dlq");
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_topics.ServiceBReady); // ONLY the enriched topic
        _logger.LogInformation("Service B subscribed to {Topic}", _topics.ServiceBReady);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<string, ServiceBCommand> result;
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

                await ProcessAsync(result, cancellationToken);
                _consumer.Commit(result); // commit after success or DLQ — either way the record is handled
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    private async Task ProcessAsync(ConsumeResult<string, ServiceBCommand> result, CancellationToken cancellationToken)
    {
        var command = result.Message.Value;
        using var scope = LogScope.Correlation(_logger, command.CorrelationId);

        for (var attempt = 1; attempt <= _options.MaxAttempts; attempt++)
        {
            try
            {
                await _handler.HandleAsync(command, cancellationToken);
                _metrics.RecordProcessed();
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Service B attempt {Attempt}/{Max} failed for {Key}",
                    attempt, _options.MaxAttempts, command.CorrelationId);

                if (attempt >= _options.MaxAttempts)
                {
                    await SendToDeadLetterAsync(result, ex, cancellationToken);
                    return;
                }

                _metrics.RecordRetried();
                await Task.Delay(_options.RetryDelayMs * attempt, cancellationToken);
            }
        }
    }

    private async Task SendToDeadLetterAsync(ConsumeResult<string, ServiceBCommand> result, Exception error, CancellationToken cancellationToken)
    {
        var headers = new Headers
        {
            { KafkaHeaders.CorrelationId, Encoding.UTF8.GetBytes(result.Message.Value.CorrelationId) },
            { KafkaHeaders.ErrorReason, Encoding.UTF8.GetBytes(error.Message) },
            { KafkaHeaders.OriginTopic, Encoding.UTF8.GetBytes(result.Topic) },
        };

        await _dlqProducer.ProduceAsync(_topics.ServiceBDlq, new Message<string, ServiceBCommand>
        {
            Key = result.Message.Key,
            Value = result.Message.Value,
            Headers = headers,
        }, cancellationToken);

        _metrics.RecordDeadLettered();
        _logger.LogError("Service B exhausted retries for {Key}; routed to DLQ {Dlq}",
            result.Message.Value.CorrelationId, _topics.ServiceBDlq);
    }

    public void Dispose()
    {
        _consumer.Dispose();
        _dlqProducer.Dispose();
    }
}
