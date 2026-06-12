using FluentAssertions;
using Poc.Contracts;
using Xunit;

namespace Poc.IntegrationTests;

[Collection(KafkaCollection.Name)]
public sealed class EnricherTests
{
    private static readonly TimeSpan Short = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Long = TimeSpan.FromSeconds(20);

    private readonly KafkaFixture _fixture;

    public EnricherTests(KafkaFixture fixture) => _fixture = fixture;

    // Case 1: request arrives first, completion later -> ServiceBCommand emitted.
    [Fact]
    public async Task RequestFirst_ThenCompletion_Emits()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        await using var enricher = harness.StartEnricher();
        var cid = $"req-{harness.Suffix}";

        await harness.ProduceRequestAsync(TestData.Request(cid));
        await enricher.WaitForMaterializedAsync(requests: 1, completions: 0, Long);

        harness.ConsumeOne<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Short)
            .Should().BeNull("the completion has not arrived yet");

        await harness.ProduceCompletionAsync(TestData.Completion(cid));

        var command = harness.ConsumeOne<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Long);
        command.Should().NotBeNull();
        command!.CorrelationId.Should().Be(cid);
        command.OriginalPayload.CustomerExternalId.Should().Be("cust-ext-001");
        command.ServiceAIds.OperationId.Should().Be($"op-{cid}");
    }

    // Case 2: completion arrives first, request later -> ServiceBCommand emitted.
    [Fact]
    public async Task CompletionFirst_ThenRequest_Emits()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        await using var enricher = harness.StartEnricher();
        var cid = $"comp-{harness.Suffix}";

        await harness.ProduceCompletionAsync(TestData.Completion(cid));
        await enricher.WaitForMaterializedAsync(requests: 0, completions: 1, Long);

        harness.ConsumeOne<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Short)
            .Should().BeNull("the request has not arrived yet");

        await harness.ProduceRequestAsync(TestData.Request(cid));

        var command = harness.ConsumeOne<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Long);
        command.Should().NotBeNull();
        command!.CorrelationId.Should().Be(cid);
    }

    // Case 3: duplicate request event does not produce a duplicate final command.
    [Fact]
    public async Task DuplicateRequest_DoesNotDuplicateOutput()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        await using var enricher = harness.StartEnricher();
        var cid = $"dupreq-{harness.Suffix}";

        await harness.ProduceRequestAsync(TestData.Request(cid));
        await harness.ProduceCompletionAsync(TestData.Completion(cid));
        await harness.ProduceRequestAsync(TestData.Request(cid)); // duplicate after the join already fired

        harness.CountWithin<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Short)
            .Should().Be(1);
    }

    // Case 4: duplicate Service A completion does not produce a duplicate final command.
    [Fact]
    public async Task DuplicateCompletion_DoesNotDuplicateOutput()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        await using var enricher = harness.StartEnricher();
        var cid = $"dupcomp-{harness.Suffix}";

        await harness.ProduceRequestAsync(TestData.Request(cid));
        await harness.ProduceCompletionAsync(TestData.Completion(cid));
        await harness.ProduceCompletionAsync(TestData.Completion(cid)); // duplicate completion

        harness.CountWithin<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Short)
            .Should().Be(1);
    }

    // Case 5: missing Service A completion does not emit anything to Service B.
    [Fact]
    public async Task MissingCompletion_EmitsNothing()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        await using var enricher = harness.StartEnricher();
        var cid = $"lonely-{harness.Suffix}";

        await harness.ProduceRequestAsync(TestData.Request(cid));
        await enricher.WaitForMaterializedAsync(requests: 1, completions: 0, Long);

        harness.ConsumeOne<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Short)
            .Should().BeNull();
    }

    // Case 8: enricher restart does not lose pending records.
    [Fact]
    public async Task EnricherRestart_PreservesPendingRequest()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        var cid = $"restart-{harness.Suffix}";
        var group = $"enricher-restart-{harness.Suffix}";

        var first = harness.StartEnricher(group);
        await harness.ProduceRequestAsync(TestData.Request(cid));
        await first.WaitForMaterializedAsync(requests: 1, completions: 0, Long);
        await first.DisposeAsync(); // simulate a crash/restart

        await using var second = harness.StartEnricher(group); // same state path + group
        await harness.ProduceCompletionAsync(TestData.Completion(cid));

        var command = harness.ConsumeOne<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Long);
        command.Should().NotBeNull("the pending request must survive the restart via durable state");
        command!.CorrelationId.Should().Be(cid);
    }

    // Case 9: the same correlationId is preserved across the join.
    [Fact]
    public async Task CorrelationId_IsPreservedThroughJoin()
    {
        await using var harness = await PipelineHarness.CreateAsync(_fixture);
        await using var enricher = harness.StartEnricher();
        var cid = $"corr-{harness.Suffix}";

        await harness.ProduceRequestAsync(TestData.Request(cid));
        await harness.ProduceCompletionAsync(TestData.Completion(cid));

        var command = harness.ConsumeOne<ServiceBCommand>(harness.Options.Topics.ServiceBReady, Long);
        command.Should().NotBeNull();
        command!.CorrelationId.Should().Be(cid, "key = correlationId must flow request -> completion -> ready");
    }
}
