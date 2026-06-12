using Poc.Contracts;

namespace Poc.ServiceB.Worker;

/// <summary>Business processing seam for Service B. Swapped out in tests to force failures.</summary>
public interface IServiceBHandler
{
    Task HandleAsync(ServiceBCommand command, CancellationToken cancellationToken);
}

/// <summary>
/// PoC handler that simulates work. To make the DLQ path testable end-to-end, it throws when the
/// description signals a poison message (contains "fail"), exercising retry exhaustion → DLQ.
/// </summary>
public sealed class SimulatedServiceBHandler : IServiceBHandler
{
    private readonly ILogger<SimulatedServiceBHandler> _logger;

    public SimulatedServiceBHandler(ILogger<SimulatedServiceBHandler> logger) => _logger = logger;

    public async Task HandleAsync(ServiceBCommand command, CancellationToken cancellationToken)
    {
        if (command.OriginalPayload.Description.Contains("fail", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Simulated processing failure for {command.CorrelationId}");
        }

        await Task.Delay(Random.Shared.Next(10, 40), cancellationToken);
        _logger.LogInformation("Service B processed command for operationId {OperationId}, amount {Amount}",
            command.ServiceAIds.OperationId, command.OriginalPayload.Amount);
    }
}
