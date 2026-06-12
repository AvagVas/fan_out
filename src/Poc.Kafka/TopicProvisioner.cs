using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;

namespace Poc.Kafka;

/// <summary>
/// Creates the four pipeline topics on startup if they do not already exist. The two KTable source
/// topics are log-compacted (latest value per correlationId is what the join materializes); the
/// output and DLQ topics use normal delete retention. Acts as a backstop to the compose init job.
/// </summary>
public sealed class TopicProvisioner
{
    private readonly KafkaClientFactory _factory;
    private readonly ILogger<TopicProvisioner> _logger;

    public TopicProvisioner(KafkaClientFactory factory, ILogger<TopicProvisioner> logger)
    {
        _factory = factory;
        _logger = logger;
    }

    public async Task EnsureTopicsAsync(int partitions = 6, short replicationFactor = 1, CancellationToken cancellationToken = default)
    {
        var topics = _factory.Options.Topics;
        var specs = new[]
        {
            Compacted(topics.ProxyRequests, partitions, replicationFactor),
            Compacted(topics.ServiceACompleted, partitions, replicationFactor),
            Deleted(topics.ServiceBReady, partitions, replicationFactor),
            Deleted(topics.ServiceBDlq, partitions, replicationFactor),
        };

        using var admin = _factory.CreateAdminClient();
        try
        {
            await admin.CreateTopicsAsync(specs);
            _logger.LogInformation("Ensured Kafka topics: {Topics}", string.Join(", ", specs.Select(s => s.Name)));
        }
        catch (CreateTopicsException ex)
        {
            foreach (var result in ex.Results)
            {
                if (result.Error.Code == ErrorCode.TopicAlreadyExists)
                {
                    _logger.LogDebug("Topic {Topic} already exists", result.Topic);
                }
                else if (result.Error.IsError)
                {
                    _logger.LogError("Failed to create topic {Topic}: {Reason}", result.Topic, result.Error.Reason);
                    throw;
                }
            }
        }
    }

    private static TopicSpecification Compacted(string name, int partitions, short rf) => new()
    {
        Name = name,
        NumPartitions = partitions,
        ReplicationFactor = rf,
        Configs = new Dictionary<string, string>
        {
            ["cleanup.policy"] = "compact",
            ["min.cleanable.dirty.ratio"] = "0.1",
        },
    };

    private static TopicSpecification Deleted(string name, int partitions, short rf) => new()
    {
        Name = name,
        NumPartitions = partitions,
        ReplicationFactor = rf,
        Configs = new Dictionary<string, string> { ["cleanup.policy"] = "delete" },
    };
}
