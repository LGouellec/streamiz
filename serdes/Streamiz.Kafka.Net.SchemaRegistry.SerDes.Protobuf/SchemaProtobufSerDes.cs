using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf
{
    /// <summary>
    /// SerDes for Protobuf
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SchemaProtobufSerDes<T> : SchemaSerDes<T> where T : class, IMessage<T>, new()
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

        private Confluent.SchemaRegistry.Serdes.ProtobufSerializerConfig GetSerializerConfig(ISchemaRegistryConfig config)
        {
            Confluent.SchemaRegistry.Serdes.ProtobufSerializerConfig c = new Confluent.SchemaRegistry.Serdes.ProtobufSerializerConfig();
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
                if (context.Config is ISchemaRegistryConfig schemaConfig)
                {
                    registryClient = GetSchemaRegistryClient(GetConfig(schemaConfig));
                    deserializer = new ProtobufDeserializer<T>(schemaConfig as Config);
                    serializer = new ProtobufSerializer<T>(registryClient, GetSerializerConfig(schemaConfig));

                    isInitialized = true;
                }
                else
                {
                    throw new StreamConfigException($"Configuration must inherited from ISchemaRegistryConfig for SchemaProtobufSerDes<{typeof(T).Name}");
                }
            }
        }
    }
}