using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;
using Poc.Contracts;
using Poc.Kafka;
using Poc.Proxy.Api;

var builder = WebApplication.CreateBuilder(args);

builder.AddPocObservability("proxy-api");
builder.Services.AddPocKafka(builder.Configuration);

// Idempotent producer for the raw request topic; disposed with the container on shutdown.
builder.Services.AddSingleton(sp => sp.GetRequiredService<KafkaClientFactory>().CreateProducer<RequestReceived>("proxy-api"));
builder.Services.AddHostedService<TopicProvisionHostedService>();

var attemptTimeoutSec = builder.Configuration.GetValue("ServiceA:AttemptTimeoutSeconds", 30);
builder.Services.AddHttpClient<ServiceAClient>(client =>
{
    client.BaseAddress = new Uri(builder.Configuration["ServiceA:BaseUrl"] ?? "http://localhost:5081");
    client.Timeout = Timeout.InfiniteTimeSpan; // the resilience pipeline governs timeouts
}).AddStandardResilienceHandler(options =>
{
    // Each attempt must outlast Service A's (possibly simulated-slow) processing, or it cancels the
    // call before Service A can emit its completion. Constraints: Total >= Attempt; CB sampling >= 2x Attempt.
    options.AttemptTimeout.Timeout = TimeSpan.FromSeconds(attemptTimeoutSec);
    options.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(attemptTimeoutSec * 2);
    options.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(attemptTimeoutSec * 3 + 10);
}); // retry + timeout + circuit breaker for the synchronous Service A call

builder.Services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka");

var app = builder.Build();

app.MapPrometheusScrapingEndpoint();
app.MapHealthChecks("/health");

app.MapPost("/api/requests", async (
    CreateRequestDto dto,
    IProducer<string, RequestReceived> producer,
    ServiceAClient serviceA,
    KafkaClientFactory kafka,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    // 1. Validate.
    var validation = new List<ValidationResult>();
    if (!Validator.TryValidateObject(dto, new ValidationContext(dto), validation, validateAllProperties: true))
    {
        return Results.ValidationProblem(validation.ToDictionary(v => v.MemberNames.FirstOrDefault() ?? "", v => new[] { v.ErrorMessage ?? "invalid" }));
    }

    // 2. Generate correlation id and stamp every log line with it.
    var correlationId = Guid.NewGuid().ToString();
    using var scope = LogScope.Correlation(logger, correlationId);

    var payload = new Payload
    {
        CustomerExternalId = dto.CustomerExternalId,
        Amount = dto.Amount,
        Description = dto.Description,
    };

    // 3. Publish the raw request to Kafka (in parallel with the synchronous flow). Await the ack so we
    //    know it is durably persisted before relying on the downstream pipeline.
    var requestEvent = new RequestReceived
    {
        CorrelationId = correlationId,
        RequestId = Guid.NewGuid().ToString(),
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Payload = payload,
    };

    var delivery = await producer.ProduceAsync(
        kafka.Options.Topics.ProxyRequests,
        new Message<string, RequestReceived>
        {
            Key = correlationId, // key = correlationId for all related topics
            Value = requestEvent,
            Headers = new Headers { { KafkaHeaders.CorrelationId, System.Text.Encoding.UTF8.GetBytes(correlationId) } },
        },
        cancellationToken);

    logger.LogInformation("Published RequestReceived to {Topic} at offset {Offset}", delivery.Topic, delivery.Offset.Value);

    // 4. Call Service A synchronously and wait for it to finish.
    var serviceAResponse = await serviceA.ProcessAsync(correlationId, payload, cancellationToken);
    logger.LogInformation("Service A completed with operationId {OperationId}", serviceAResponse.ServiceAIds.OperationId);

    // 5. Return Service A's response (plus the correlation id) to the caller.
    return Results.Ok(new CreateRequestResponse(correlationId, serviceAResponse.Status, serviceAResponse.ServiceAIds));
});

app.Run();

public partial class Program;
