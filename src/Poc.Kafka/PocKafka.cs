using System.Diagnostics;

namespace Poc.Kafka;

/// <summary>Kafka header names used for cross-service correlation propagation.</summary>
public static class KafkaHeaders
{
    public const string CorrelationId = "x-correlation-id";
    public const string ErrorReason = "x-error-reason";
    public const string OriginTopic = "x-origin-topic";
}

/// <summary>Centralized OpenTelemetry source/meter names so producers and exporters agree.</summary>
public static class PocDiagnostics
{
    public const string ActivitySourceName = "Poc.ConfluentLab";
    public const string EnricherMeterName = "Poc.Enricher";
    public const string ServiceBMeterName = "Poc.ServiceB";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}

/// <summary>Logger scope key used to stamp every log line with the active correlation id.</summary>
public static class LogScope
{
    public const string CorrelationIdKey = "CorrelationId";

    public static IDisposable? Correlation(Microsoft.Extensions.Logging.ILogger logger, string correlationId) =>
        logger.BeginScope(new Dictionary<string, object> { [CorrelationIdKey] = correlationId });
}
