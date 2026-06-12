using System.Diagnostics.Metrics;
using Poc.Kafka;

namespace Poc.Enricher.Streams;

/// <summary>
/// OpenTelemetry metrics for the join. Counters track throughput/health; observable gauges read the
/// durable state store so pending backlogs are always accurate, even after a restart.
/// </summary>
public sealed class EnricherMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _joined;
    private readonly Counter<long> _failed;
    private readonly Counter<long> _duplicate;
    private readonly Counter<long> _unmatched;

    public EnricherMetrics(EnricherStateStore state)
    {
        _meter = new Meter(PocDiagnostics.EnricherMeterName);
        _joined = _meter.CreateCounter<long>("enricher_joined_records", description: "Successful joins emitted to service-b.ready");
        _failed = _meter.CreateCounter<long>("enricher_failed_records", description: "Records that failed processing");
        _duplicate = _meter.CreateCounter<long>("enricher_duplicate_records", description: "Records for an already-emitted correlationId");
        _unmatched = _meter.CreateCounter<long>("enricher_unmatched_records", description: "Records still waiting for their counterpart");

        // Pending = materialized on one side but not yet joined. Every emitted key has both sides.
        _meter.CreateObservableGauge("enricher_pending_requests", () => state.CountRequests() - state.CountEmitted());
        _meter.CreateObservableGauge("enricher_pending_completions", () => state.CountCompletions() - state.CountEmitted());
        _meter.CreateObservableGauge("enricher_joined_total_state", () => state.CountEmitted());
    }

    public void RecordJoined() => _joined.Add(1);

    public void RecordFailed() => _failed.Add(1);

    public void RecordDuplicate() => _duplicate.Add(1);

    public void RecordUnmatched() => _unmatched.Add(1);

    public void Dispose() => _meter.Dispose();
}
