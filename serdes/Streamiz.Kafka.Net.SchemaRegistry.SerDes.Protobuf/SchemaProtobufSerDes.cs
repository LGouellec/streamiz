using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Tests,PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]
namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf
{
    /// <summary>
    /// SerDes for Protobuf
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SchemaProtobufSerDes<T> : SchemaSerDes<T, ProtobufSerializerConfig> 
        where T : class, IMessage<T>, new()
    {
        /// <summary>
        /// Empty constructor
        /// </summary>
        public SchemaProtobufSerDes()
            : base("protobuf")
        { }

        protected override ProtobufSerializerConfig GetSerializerConfig(ISchemaRegistryConfig config)
        {
            ProtobufSerializerConfig protobufConfig = base.GetSerializerConfig(config);
            if (config.UseDeprecatedFormat.HasValue)
                protobufConfig.UseDeprecatedFormat = config.UseDeprecatedFormat.Value;
            if (config.SkipKnownTypes.HasValue)
                protobufConfig.SkipKnownTypes = config.SkipKnownTypes.Value;
            if (config.ReferenceSubjectNameStrategy.HasValue)
                protobufConfig.ReferenceSubjectNameStrategy = (Confluent.SchemaRegistry.ReferenceSubjectNameStrategy)config.ReferenceSubjectNameStrategy.Value;
            return protobufConfig;
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