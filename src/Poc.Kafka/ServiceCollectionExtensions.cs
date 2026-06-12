using Confluent.SchemaRegistry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Poc.Kafka;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers Kafka options (with env overrides), the Schema Registry client, the client factory,
    /// and the topic provisioner. Shared by every service so config and security are uniform.
    /// </summary>
    public static IServiceCollection AddPocKafka(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .PostConfigure(options => options.ApplyEnvironmentOverrides())
            .ValidateOnStart();

        services.AddSingleton<ISchemaRegistryClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = new SchemaRegistryConfig { Url = options.SchemaRegistryUrl };
            if (!string.IsNullOrWhiteSpace(options.SchemaRegistryApiKey))
            {
                config.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
                config.BasicAuthUserInfo = $"{options.SchemaRegistryApiKey}:{options.SchemaRegistryApiSecret}";
            }

            return new CachedSchemaRegistryClient(config);
        });

        services.AddSingleton<KafkaClientFactory>();
        services.AddSingleton<TopicProvisioner>();
        return services;
    }
}
