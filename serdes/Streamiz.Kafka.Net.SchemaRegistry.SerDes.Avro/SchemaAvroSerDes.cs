using Confluent.SchemaRegistry.Serdes;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro
{
    /// <summary>
    /// SerDes for avro beans
    /// </summary>
    /// <typeparam name="T">type of avro bean
    /// </typeparam>
    public class SchemaAvroSerDes<T> : SchemaSerDes<T>
    {
        private Confluent.SchemaRegistry.SchemaRegistryConfig GetConfig(ISchemaRegistryConfig config)
        {
            Confluent.SchemaRegistry.SchemaRegistryConfig c = new Confluent.SchemaRegistry.SchemaRegistryConfig();
            c.Url = config.SchemaRegistryUrl;
            if (config.SchemaRegistryMaxCachedSchemas.HasValue)
            {
                c.MaxCachedSchemas = config.SchemaRegistryMaxCachedSchemas;
            }

            if (config.SchemaRegistryRequestTimeoutMs.HasValue)
            {
                c.RequestTimeoutMs = config.SchemaRegistryRequestTimeoutMs;
            }

            return c;
        }

        private Confluent.SchemaRegistry.Serdes.AvroSerializerConfig GetSerializerConfig(ISchemaRegistryConfig config)
        {
            Confluent.SchemaRegistry.Serdes.AvroSerializerConfig c = new Confluent.SchemaRegistry.Serdes.AvroSerializerConfig();
            if (config.AutoRegisterSchemas.HasValue)
            {
                c.AutoRegisterSchemas = config.AutoRegisterSchemas;
            }
            if (config.SubjectNameStrategy.HasValue)
            {
                c.SubjectNameStrategy = (Confluent.SchemaRegistry.SubjectNameStrategy)config.SubjectNameStrategy.Value;
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
            if (!isInitialized)
            {
                if (context.Config is ISchemaRegistryConfig)
                {
                    var schemaConfig = context.Config as ISchemaRegistryConfig;

                    registryClient = GetSchemaRegistryClient(GetConfig(schemaConfig));
                    deserializer = new AvroDeserializer<T>(registryClient);
                    serializer = new AvroSerializer<T>(registryClient, GetSerializerConfig(schemaConfig));

                    isInitialized = true;
                }
                else
                {
                    throw new StreamConfigException($"Configuration must inherited from ISchemaRegistryConfig for SchemaAvroSerDes<{typeof(T).Name}");
                }
            }
        }
    }
}