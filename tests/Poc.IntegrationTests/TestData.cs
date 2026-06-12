using Poc.Contracts;

namespace Poc.IntegrationTests;

internal static class TestData
{
    public static RequestReceived Request(string correlationId, string description = "integration test", decimal amount = 42.50m) => new()
    {
        CorrelationId = correlationId,
        RequestId = Guid.NewGuid().ToString(),
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Payload = new Payload
        {
            CustomerExternalId = "cust-ext-001",
            Amount = amount,
            Description = description,
        },
    };

    public static ServiceACompleted Completion(string correlationId) => new()
    {
        CorrelationId = correlationId,
        CompletedAtUtc = DateTimeOffset.UtcNow,
        ServiceAIds = new ServiceAIds
        {
            CustomerId = $"cust-{correlationId}",
            OperationId = $"op-{correlationId}",
            InternalRequestId = $"intreq-{correlationId}",
        },
    };

    public static ServiceBCommand ReadyCommand(string correlationId, string description = "ready") => new()
    {
        CorrelationId = correlationId,
        CreatedAtUtc = DateTimeOffset.UtcNow,
        OriginalPayload = new Payload { CustomerExternalId = "cust-ext-001", Amount = 10m, Description = description },
        ServiceAIds = new ServiceAIds { CustomerId = "c", OperationId = "o", InternalRequestId = "i" },
    };
}
