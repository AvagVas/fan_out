using Microsoft.Extensions.Options;
using Newtonsoft.Json.Serialization;
using NJsonSchema.NewtonsoftJson.Generation;
using Poc.Contracts;
using Poc.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json;
using Streamiz.Kafka.Net.SerDes;

namespace Poc.Enricher.Streamiz;

/// <summary>
/// The Kafka Streams DSL version of the Enricher, using Streamiz.Kafka.Net. It expresses the join as
/// an actual <c>KTable ⋈ KTable</c> over two materialized tables — the same topology the spec sketches:
///
///   var requests    = builder.Table("proxy.requests");
///   var completions  = builder.Table("service-a.completed");
///   requests.Join(completions, (r, c) => new ServiceBCommand {...}).ToStream().To("service-b.ready");
///
/// Streamiz owns the state stores (RocksDB), changelog topics, offset management and rebalancing.
///
/// Difference vs. the manual enricher: a raw KTable-KTable join re-emits on every same-key update, so
/// it does NOT inherently dedupe duplicate inputs — that is the trade-off for the concise DSL. The
/// manual enricher adds an explicit "emitted" ledger to guarantee exactly-once output per correlationId.
/// </summary>
public sealed class StreamizEnricherService : IHostedService
{
    private readonly KafkaOptions _options;
    private readonly IConfiguration _configuration;
    private readonly ILogger<StreamizEnricherService> _logger;
    private KafkaStream? _stream;

    public StreamizEnricherService(IOptions<KafkaOptions> options, IConfiguration configuration, ILogger<StreamizEnricherService> logger)
    {
        _options = options.Value;
        _configuration = configuration;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var topics = _options.Topics;

        var config = new StreamConfig
        {
            ApplicationId = _configuration["Enricher:ApplicationId"] ?? "enricher-streamiz",
            BootstrapServers = _options.BootstrapServers,
            SchemaRegistryUrl = _options.SchemaRegistryUrl,
            AutoRegisterSchemas = true,
        };
        if (!string.IsNullOrWhiteSpace(_options.SchemaRegistryApiKey))
        {
            config.BasicAuthUserInfo = $"{_options.SchemaRegistryApiKey}:{_options.SchemaRegistryApiSecret}";
        }

        // camelCase JSON to match the contracts produced by Proxy/Service A and read by Service B.
        var jsonSettings = new NewtonsoftJsonSchemaGeneratorSettings
        {
            SerializerSettings = new Newtonsoft.Json.JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
            },
        };

        var builder = new StreamBuilder();

        // Materialize both input topics as KTables.
        var requests = builder.Table(
            topics.ProxyRequests,
            new StringSerDes(),
            new SchemaJsonSerDes<RequestReceived>(jsonSettings));

        var completions = builder.Table(
            topics.ServiceACompleted,
            new StringSerDes(),
            new SchemaJsonSerDes<ServiceACompleted>(jsonSettings));

        // KTable-KTable inner join on the key (correlationId).
        requests
            .Join(completions, (RequestReceived request, ServiceACompleted completion) => new ServiceBCommand
            {
                CorrelationId = request.CorrelationId,
                CreatedAtUtc = DateTimeOffset.UtcNow,
                OriginalPayload = request.Payload,
                ServiceAIds = completion.ServiceAIds,
            })
            .ToStream()
            .To(topics.ServiceBReady, new StringSerDes(), new SchemaJsonSerDes<ServiceBCommand>(jsonSettings));

        var topology = builder.Build();
        _stream = new KafkaStream(topology, config);

        _logger.LogInformation(
            "Starting Streamiz KTable-KTable enricher (application.id={AppId}): {Requests} JOIN {Completions} -> {Ready}",
            config.ApplicationId, topics.ProxyRequests, topics.ServiceACompleted, topics.ServiceBReady);

        await _stream.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _stream?.Dispose(); // graceful: flush state stores, leave the group
        return Task.CompletedTask;
    }
}
