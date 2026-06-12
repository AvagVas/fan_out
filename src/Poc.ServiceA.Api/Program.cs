using Poc.Contracts;
using Poc.Kafka;
using Poc.ServiceA.Api;

var builder = WebApplication.CreateBuilder(args);

// Simulated processing delay (ms). Configurable via ServiceA:ProcessingDelayMs or env SERVICEA__PROCESSINGDELAYMS.
var processingDelayMs = builder.Configuration.GetValue("ServiceA:ProcessingDelayMs", 3000);

builder.AddPocObservability("service-a-api");
builder.Services.AddPocKafka(builder.Configuration);
builder.Services.AddSingleton(sp => sp.GetRequiredService<KafkaClientFactory>().CreateProducer<ServiceACompleted>("service-a-api"));
builder.Services.AddSingleton<IOutbox, DirectKafkaOutbox>();
builder.Services.AddHostedService<TopicProvisionHostedService>();
builder.Services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka");

var app = builder.Build();

app.MapPrometheusScrapingEndpoint();
app.MapHealthChecks("/health");

app.MapPost("/api/process", async (
    ProcessRequest request,
    IOutbox outbox,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    var correlationId = request.CorrelationId;
    using var scope = LogScope.Correlation(logger, correlationId);

    // 2. Simulate business processing (configurable delay).
    logger.LogInformation("Service A processing request for customer {Customer} (delay {Delay}ms)",
        request.Payload.CustomerExternalId, processingDelayMs);
    await Task.Delay(processingDelayMs, cancellationToken);

    // 3. Generate the IDs Service B will need.
    var serviceAIds = new ServiceAIds
    {
        CustomerId = $"cust-{Guid.NewGuid():N}",
        OperationId = $"op-{Guid.NewGuid():N}",
        InternalRequestId = $"intreq-{Guid.NewGuid():N}",
    };

    // 4. Emit ServiceACompleted via the outbox seam (same correlationId as the request).
    var completed = new ServiceACompleted
    {
        CorrelationId = correlationId,
        CompletedAtUtc = DateTimeOffset.UtcNow,
        ServiceAIds = serviceAIds,
    };
    await outbox.PublishAsync(completed, cancellationToken);

    // 5. Return a synchronous response to the Proxy.
    return Results.Ok(new ServiceAResponse(correlationId, completed.CompletedAtUtc, serviceAIds, "completed"));
});

app.Run();

public partial class Program;
