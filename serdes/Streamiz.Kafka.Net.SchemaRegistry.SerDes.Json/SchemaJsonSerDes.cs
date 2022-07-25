using System.Runtime.CompilerServices;
using Confluent.SchemaRegistry.Serdes;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;

[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Tests,PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]
namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json
{
    /// <summary>
    /// SerDes for json beans
    /// </summary>
    /// <typeparam name="T">type of json bean
    /// </typeparam>
    public class SchemaJsonSerDes<T> : SchemaSerDes<T, JsonSerializerConfig>
        where T : class
    {
        /// <summary>
        /// Empty constructor
        /// </summary>
        public SchemaJsonSerDes() 
            : base("json")
        {
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
                    deserializer = new JsonDeserializer<T>();
                    serializer = new JsonSerializer<T>(registryClient, GetSerializerConfig(schemaConfig));
                    isInitialized = true;
                }
                else
                {
                    throw new StreamConfigException(
                        $"Configuration must inherited from ISchemaRegistryConfig for SchemaAvroSerDes<{typeof(T).Name}");
                }
            }
        }
        
    }
}