using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Poc.Contracts;
using Poc.ServiceB.Worker;
using Xunit;

namespace Poc.IntegrationTests;

[Collection(KafkaCollection.Name)]
public sealed class ServiceBTests
{
    private static readonly TimeSpan Long = TimeSpan.FromSeconds(20);

    private readonly KafkaFixture _fixture;

    public ServiceBTests(KafkaFixture fixture) => _fixture = fixture;

    // Case 6: Service B consumes only service-b.ready and ignores proxy.requests.
    [Fact]
    public async Task ServiceB_ConsumesOnlyReadyTopic()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        var handler = new RecordingHandler();
        await using var worker = ServiceBRunner.Start(harness, handler);

        var cid = $"sb-only-{harness.Suffix}";
        await harness.ProduceReadyAsync(TestData.ReadyCommand(cid));
        // Also place a raw request on proxy.requests — Service B must never see it.
        await harness.ProduceRequestAsync(TestData.Request($"raw-{harness.Suffix}"));

        await handler.WaitForCountAsync(1, Long);
        await Task.Delay(2000); // give any erroneous extra consumption a chance to show up

        handler.Commands.Should().ContainSingle();
        handler.Commands.Single().CorrelationId.Should().Be(cid);
    }

    // Case 7: Service B failure after retry exhaustion routes the message to service-b.dlq.
    [Fact]
    public async Task ServiceB_Failure_RoutesToDlq()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        var handler = new SimulatedServiceBHandler(NullLogger<SimulatedServiceBHandler>.Instance);
        await using var worker = ServiceBRunner.Start(harness, handler, maxAttempts: 2, retryDelayMs: 50);

        var cid = $"sb-dlq-{harness.Suffix}";
        await harness.ProduceReadyAsync(TestData.ReadyCommand(cid, description: "please fail this one"));

        var dead = harness.ConsumeOne<ServiceBCommand>(harness.Options.Topics.ServiceBDlq, Long);
        dead.Should().NotBeNull("after retries are exhausted the message must land in the DLQ");
        dead!.CorrelationId.Should().Be(cid);
    }
}

internal sealed class RecordingHandler : IServiceBHandler
{
    public ConcurrentBag<ServiceBCommand> Commands { get; } = new();

    public Task HandleAsync(ServiceBCommand command, CancellationToken cancellationToken)
    {
        Commands.Add(command);
        return Task.CompletedTask;
    }

    public async Task WaitForCountAsync(int count, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (Commands.Count >= count)
            {
                return;
            }

            await Task.Delay(100);
        }
    }
}

internal sealed class ServiceBRunner : IAsyncDisposable
{
    private readonly ServiceBConsumer _consumer;
    private readonly ServiceBMetrics _metrics;
    private readonly CancellationTokenSource _cts;
    private readonly Task _task;

    private ServiceBRunner(ServiceBConsumer consumer, ServiceBMetrics metrics, CancellationTokenSource cts, Task task)
    {
        _consumer = consumer;
        _metrics = metrics;
        _cts = cts;
        _task = task;
    }

    public static ServiceBRunner Start(PipelineHarness harness, IServiceBHandler handler, int maxAttempts = 3, int retryDelayMs = 100)
    {
        var metrics = new ServiceBMetrics();
        var options = new ServiceBOptions
        {
            GroupId = $"service-b-{harness.Suffix}",
            MaxAttempts = maxAttempts,
            RetryDelayMs = retryDelayMs,
        };
        var consumer = new ServiceBConsumer(harness.Kafka, handler, metrics, NullLogger<ServiceBConsumer>.Instance, options);
        var cts = new CancellationTokenSource();
        var task = Task.Run(() => consumer.RunAsync(cts.Token));
        return new ServiceBRunner(consumer, metrics, cts, task);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _task; } catch { /* expected on cancel */ }
        _consumer.Dispose();
        _metrics.Dispose();
        _cts.Dispose();
    }
}
