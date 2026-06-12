namespace Poc.Enricher.Streams;

/// <summary>Thin hosted-service wrapper that runs the join loop and stops it on graceful shutdown.</summary>
public sealed class EnricherWorker : BackgroundService
{
    private readonly EnricherProcessor _processor;

    public EnricherWorker(EnricherProcessor processor) => _processor = processor;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => _processor.RunAsync(stoppingToken);
}
