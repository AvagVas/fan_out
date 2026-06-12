using System.ComponentModel.DataAnnotations;
using Poc.Contracts;

namespace Poc.Proxy.Api;

/// <summary>Inbound client request to <c>POST /api/requests</c> (System.Text.Json camelCase).</summary>
public sealed class CreateRequestDto
{
    [Required]
    public string CustomerExternalId { get; set; } = string.Empty;

    [Range(0.01, 1_000_000_000)]
    public decimal Amount { get; set; }

    [Required]
    public string Description { get; set; } = string.Empty;
}

/// <summary>Body the Proxy posts to Service A's <c>/api/process</c>.</summary>
public sealed record ProcessRequest(string CorrelationId, Payload Payload);

/// <summary>Synchronous response returned by Service A.</summary>
public sealed record ServiceAResponse(string CorrelationId, DateTimeOffset CompletedAtUtc, ServiceAIds ServiceAIds, string Status);

/// <summary>What the Proxy returns to the original HTTP caller.</summary>
public sealed record CreateRequestResponse(string CorrelationId, string Status, ServiceAIds ServiceAIds);
