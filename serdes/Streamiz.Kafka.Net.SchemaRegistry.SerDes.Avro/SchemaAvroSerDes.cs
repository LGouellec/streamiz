using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using System;

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

                    registryClient = GetSchemaRegistryClient(GetConfig(schemaConfig));
                    avroDeserializer = new AvroDeserializer<T>(registryClient);
                    avroSerializer = new AvroSerializer<T>(registryClient, GetSerializerConfig(schemaConfig));

                    isInitialized = true;
                }
                else
                    throw new StreamConfigException($"Configuration must inherited from ISchemaRegistryConfig for SchemaAvroSerDes<{typeof(T).Name}");
                    
            }
        }

        public override T Deserialize(byte[] data, SerializationContext context)
        {
            if (!isInitialized)
                throw new StreamsException($"SchemaAvroSerDes<{typeof(T).Name} is not initialized !");

            return avroDeserializer
                   .AsSyncOverAsync()
                   .Deserialize(data, data == null, context);
        }

        public override byte[] Serialize(T data, SerializationContext context)
        {
            if (!isInitialized)
                throw new StreamsException($"SchemaAvroSerDes<{typeof(T).Name} is not initialized !");

            return avroSerializer
                            .AsSyncOverAsync()
                            .Serialize(data, context);
        }
    
        protected virtual ISchemaRegistryClient GetSchemaRegistryClient(Confluent.SchemaRegistry.SchemaRegistryConfig config)
        {
            return new CachedSchemaRegistryClient(config);
        }
    }
}
