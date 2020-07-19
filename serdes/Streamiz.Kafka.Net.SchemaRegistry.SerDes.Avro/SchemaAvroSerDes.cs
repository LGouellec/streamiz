using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro
{
    public class SchemaAvroSerDes<T> : AbstractSerDes<T>
    {
        private ISchemaRegistryClient registryClient;
        private AvroSerializer<T> avroSerializer;
        private AvroDeserializer<T> avroDeserializer;

        private Confluent.SchemaRegistry.SchemaRegistryConfig GetConfig(ISchemaRegistryConfig config)
        {
            Confluent.SchemaRegistry.SchemaRegistryConfig c = new Confluent.SchemaRegistry.SchemaRegistryConfig();
            c.Url = config.SchemaRegistryUrl;
            if (config.SchemaRegistryMaxCachedSchemas.HasValue)
                c.MaxCachedSchemas = config.SchemaRegistryMaxCachedSchemas;
            if (config.SchemaRegistryRequestTimeoutMs.HasValue)
                c.RequestTimeoutMs = config.SchemaRegistryRequestTimeoutMs;
            return c;
        }

        private Confluent.SchemaRegistry.Serdes.AvroSerializerConfig GetSerializerConfig(ISchemaRegistryConfig config)
        {
            Confluent.SchemaRegistry.Serdes.AvroSerializerConfig c = new Confluent.SchemaRegistry.Serdes.AvroSerializerConfig();
            if (config.AutoRegisterSchemas.HasValue)
                c.AutoRegisterSchemas = config.AutoRegisterSchemas;
            return c;
        }

        public override void Initialize(SerDesContext context)
        {
            if (!isInitialized)
            {
                if (context.Config is ISchemaRegistryConfig)
                {
                    var schemaConfig = context.Config as ISchemaRegistryConfig;

                    registryClient = new CachedSchemaRegistryClient(GetConfig(schemaConfig));
                    avroDeserializer = new AvroDeserializer<T>(registryClient);
                    avroSerializer = new AvroSerializer<T>(registryClient, GetSerializerConfig(schemaConfig));

                    isInitialized = true;
                }
                else
                    throw new StreamConfigException($"Configuration must inherited from ISchemaRegistryConfig for SchemaAvroSerDes<{typeof(T).Name}");
                    
            }
        }

        public override T Deserialize(byte[] data)
        {
            return avroDeserializer
                   .AsSyncOverAsync()
                   .Deserialize(data, data == null, new Confluent.Kafka.SerializationContext());
        }

        public override byte[] Serialize(T data)
        {
            return avroSerializer
                            .AsSyncOverAsync()
                            .Serialize(data, new Confluent.Kafka.SerializationContext());
        }
    }
}
