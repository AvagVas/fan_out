namespace Poc.ServiceB.Worker;

/// <summary>Hosted-service wrapper that runs the Service B consume loop until shutdown.</summary>
public sealed class ServiceBWorker : BackgroundService
{
    private readonly ServiceBConsumer _consumer;

    public ServiceBWorker(ServiceBConsumer consumer) => _consumer = consumer;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => _consumer.RunAsync(stoppingToken);
}
