using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Poc.Kafka;

/// <summary>
/// Best-effort topic provisioning on startup with bounded retries, so a freshly started broker does
/// not break the service. Acts as a backstop to the compose-level kafka-init job.
/// </summary>
public sealed class TopicProvisionHostedService : BackgroundService
{
    private readonly TopicProvisioner _provisioner;
    private readonly ILogger<TopicProvisionHostedService> _logger;

    public TopicProvisionHostedService(TopicProvisioner provisioner, ILogger<TopicProvisionHostedService> logger)
    {
        _provisioner = provisioner;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        for (var attempt = 1; attempt <= 10 && !stoppingToken.IsCancellationRequested; attempt++)
        {
            try
            {
                await _provisioner.EnsureTopicsAsync(cancellationToken: stoppingToken);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Topic provisioning attempt {Attempt} failed; retrying", attempt);
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(attempt * 2, 15)), stoppingToken);
            }
        }
    }
}
