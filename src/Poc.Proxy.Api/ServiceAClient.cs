using System.Net.Http.Json;
using Poc.Contracts;
using Poc.Kafka;

namespace Poc.Proxy.Api;

/// <summary>
/// Typed HttpClient for the synchronous Service A call. Resilience (retry/timeout/circuit-breaker)
/// is attached via <c>AddStandardResilienceHandler</c> at registration time. Propagates the
/// correlation id as a header so Service A logs and the downstream completion event stay correlated.
/// </summary>
public sealed class ServiceAClient
{
    private readonly HttpClient _http;

    public ServiceAClient(HttpClient http) => _http = http;

    public async Task<ServiceAResponse> ProcessAsync(string correlationId, Payload payload, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/process")
        {
            Content = JsonContent.Create(new ProcessRequest(correlationId, payload)),
        };
        request.Headers.Add(KafkaHeaders.CorrelationId, correlationId);

        using var response = await _http.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<ServiceAResponse>(cancellationToken)
            ?? throw new InvalidOperationException("Service A returned an empty body");
    }
}
