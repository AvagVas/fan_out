using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Poc.Kafka;

public static class Observability
{
    /// <summary>
    /// Wires OpenTelemetry tracing + metrics for a service: ASP.NET Core / HttpClient instrumentation,
    /// the custom Poc meters, a Prometheus scraping endpoint, and an OTLP exporter when an endpoint is
    /// configured. Call <c>app.MapPrometheusScrapingEndpoint()</c> to expose <c>/metrics</c>.
    /// </summary>
    public static TBuilder AddPocObservability<TBuilder>(this TBuilder builder, string serviceName)
        where TBuilder : IHostApplicationBuilder
    {
        var otlpEndpoint = builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]
            ?? Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(r => r.AddService(serviceName))
            .WithTracing(tracing =>
            {
                tracing.AddAspNetCoreInstrumentation();
                tracing.AddHttpClientInstrumentation();
                tracing.AddSource(PocDiagnostics.ActivitySourceName);
                if (!string.IsNullOrWhiteSpace(otlpEndpoint))
                {
                    tracing.AddOtlpExporter();
                }
            })
            .WithMetrics(metrics =>
            {
                metrics.AddAspNetCoreInstrumentation();
                metrics.AddHttpClientInstrumentation();
                metrics.AddMeter(PocDiagnostics.EnricherMeterName);
                metrics.AddMeter(PocDiagnostics.ServiceBMeterName);
                metrics.AddPrometheusExporter();
                if (!string.IsNullOrWhiteSpace(otlpEndpoint))
                {
                    metrics.AddOtlpExporter();
                }
            });

        return builder;
    }
}
