namespace Streamiz.Demo.Infrastructure;

using Chr.Avro.Confluent;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes;
using Streamiz.Kafka.Net.SerDes;

public class AvroSerDes<T> : SchemaSerDes<T, AvroSerializerConfig>
{
    public AvroSerDes() :
        base("avro")
    {
    }

    protected override SchemaRegistryConfig GetConfig(ISchemaRegistryConfig config)
    {
        var c = new SchemaRegistryConfig
        {
            Url = config.SchemaRegistryUrl
        };

        if (config.SchemaRegistryMaxCachedSchemas.HasValue)
        {
            c.MaxCachedSchemas = config.SchemaRegistryMaxCachedSchemas;
        }

        if (config.SchemaRegistryRequestTimeoutMs.HasValue)
        {
            c.RequestTimeoutMs = config.SchemaRegistryRequestTimeoutMs;
        }

        if (!string.IsNullOrEmpty(config.BasicAuthUserInfo))
        {
            c.BasicAuthUserInfo = config.BasicAuthUserInfo;
        }

        if (config.BasicAuthCredentialsSource.HasValue)
        {
            c.BasicAuthCredentialsSource = (AuthCredentialsSource)config.BasicAuthCredentialsSource.Value;
        }

        return c;
    }

    /// <summary>
    /// Initialize method with a current context which contains <see cref="IStreamConfig"/>.
    /// Can be used to initialize the serdes according to some parameters present in the configuration such as the schema.registry.url
    /// </summary>
    /// <param name="context">SerDesContext with stream configuration</param>
    public override void Initialize(SerDesContext context)
    {
        if (isInitialized)
        {
            return;
        }

        if (context.Config is ISchemaRegistryConfig schemaConfig)
        {
            registryClient = GetSchemaRegistryClient(GetConfig(schemaConfig));
            deserializer = new AsyncSchemaRegistryDeserializer<T>(registryClient);

            AutomaticRegistrationBehavior autoRegisterSchemas =
                schemaConfig.AutoRegisterSchemas.HasValue && schemaConfig.AutoRegisterSchemas.Value
                    ? AutomaticRegistrationBehavior.Always
                    : AutomaticRegistrationBehavior.Never;

            serializer =
                new AsyncSchemaRegistrySerializer<T>(registryClient, autoRegisterSchemas);

            isInitialized = true;
        }
        else
        {
            throw new StreamConfigException(
                $"Configuration must inherited from ISchemaRegistryConfig for HostedSchemaAvroSerDes<{typeof(T).Name}");
        }
    }
}
