using Poc.Enricher.Streams;
using Poc.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.AddPocObservability("enricher");
builder.Services.AddPocKafka(builder.Configuration);

var statePath = builder.Configuration["Enricher:StatePath"] ?? "enricher-state.db";
builder.Services.AddSingleton(_ => new EnricherStateStore(statePath));
builder.Services.AddSingleton<EnricherMetrics>();
builder.Services.AddSingleton(sp => new EnricherProcessor(
    sp.GetRequiredService<KafkaClientFactory>(),
    sp.GetRequiredService<EnricherStateStore>(),
    sp.GetRequiredService<EnricherMetrics>(),
    sp.GetRequiredService<ILogger<EnricherProcessor>>(),
    builder.Configuration["Enricher:GroupId"] ?? "enricher"));

builder.Services.AddHostedService<TopicProvisionHostedService>();
builder.Services.AddHostedService<EnricherWorker>();
builder.Services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka");

var app = builder.Build();

app.MapPrometheusScrapingEndpoint();
app.MapHealthChecks("/health");

app.Run();
