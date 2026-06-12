using Poc.Enricher.Streamiz;
using Poc.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.AddPocObservability("enricher-streamiz");
builder.Services.AddPocKafka(builder.Configuration);

builder.Services.AddHostedService<TopicProvisionHostedService>();
builder.Services.AddHostedService<StreamizEnricherService>();
builder.Services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka");

var app = builder.Build();

app.MapPrometheusScrapingEndpoint();
app.MapHealthChecks("/health");

app.Run();
