using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Poc.Kafka;

/// <summary>Liveness/readiness probe that confirms the broker is reachable by fetching metadata.</summary>
public sealed class KafkaHealthCheck : IHealthCheck
{
    private readonly KafkaClientFactory _factory;

    public KafkaHealthCheck(KafkaClientFactory factory) => _factory = factory;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            using var admin = _factory.CreateAdminClient();
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
            return Task.FromResult(metadata.Brokers.Count > 0
                ? HealthCheckResult.Healthy($"{metadata.Brokers.Count} broker(s) reachable")
                : HealthCheckResult.Unhealthy("No brokers reachable"));
        }
        catch (Exception ex)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka metadata fetch failed", ex));
        }
    }
}
