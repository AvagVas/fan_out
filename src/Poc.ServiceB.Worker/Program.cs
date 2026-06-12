using Poc.Kafka;
using Poc.ServiceB.Worker;

var builder = WebApplication.CreateBuilder(args);

builder.AddPocObservability("service-b");
builder.Services.AddPocKafka(builder.Configuration);

var serviceBOptions = builder.Configuration.GetSection(ServiceBOptions.SectionName).Get<ServiceBOptions>() ?? new ServiceBOptions();
builder.Services.AddSingleton(serviceBOptions);
builder.Services.AddSingleton<ServiceBMetrics>();
builder.Services.AddSingleton<IServiceBHandler, SimulatedServiceBHandler>();
builder.Services.AddSingleton<ServiceBConsumer>();

builder.Services.AddHostedService<TopicProvisionHostedService>();
builder.Services.AddHostedService<ServiceBWorker>();
builder.Services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka");

var app = builder.Build();

app.MapPrometheusScrapingEndpoint();
app.MapHealthChecks("/health");

app.Run();
