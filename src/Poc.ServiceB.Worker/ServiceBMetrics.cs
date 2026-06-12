using System.Diagnostics.Metrics;
using Poc.Kafka;

namespace Poc.ServiceB.Worker;

public sealed class ServiceBMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _processed;
    private readonly Counter<long> _retried;
    private readonly Counter<long> _deadLettered;

    public ServiceBMetrics()
    {
        _meter = new Meter(PocDiagnostics.ServiceBMeterName);
        _processed = _meter.CreateCounter<long>("serviceb_processed_records");
        _retried = _meter.CreateCounter<long>("serviceb_retried_records");
        _deadLettered = _meter.CreateCounter<long>("serviceb_dead_lettered_records");
    }

    public void RecordProcessed() => _processed.Add(1);

    public void RecordRetried() => _retried.Add(1);

    public void RecordDeadLettered() => _deadLettered.Add(1);

    public void Dispose() => _meter.Dispose();
}
