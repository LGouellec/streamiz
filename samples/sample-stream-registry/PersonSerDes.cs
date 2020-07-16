using com.avro.bean;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Streamiz.Kafka.Net.SerDes;

namespace sample_stream_registry
{
    public class PersonSerDes : AbstractSerDes<Person>
    {
        private readonly ISchemaRegistryClient client;
        private readonly AvroDeserializer<Person> avroDeserializer;
        private readonly AvroSerializer<Person> avroSerializer;

        public PersonSerDes(SchemaRegistryConfig config)
        {
            client = new CachedSchemaRegistryClient(config);
            avroDeserializer = new AvroDeserializer<Person>(client);
            avroSerializer = new AvroSerializer<Person>(client,
                new AvroSerializerConfig
                {
                    AutoRegisterSchemas = false
                });
        }

        public override Person Deserialize(byte[] data)
        {
            return avroDeserializer
                .AsSyncOverAsync()
                .Deserialize(data, data == null, new Confluent.Kafka.SerializationContext());
        }

        public override byte[] Serialize(Person data)
        {
            return avroSerializer
                .AsSyncOverAsync()
                .Serialize(data, new Confluent.Kafka.SerializationContext());
        }
    }
}
