using Poc.Contracts;

namespace Poc.ServiceA.Api;

/// <summary>Body received from the Proxy at <c>POST /api/process</c>.</summary>
public sealed record ProcessRequest(string CorrelationId, Payload Payload);

/// <summary>Synchronous response returned to the Proxy.</summary>
public sealed record ServiceAResponse(string CorrelationId, DateTimeOffset CompletedAtUtc, ServiceAIds ServiceAIds, string Status);
