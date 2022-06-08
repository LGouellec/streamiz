using AdminClientHelpers.Confluent.Kafka;
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Schema = Confluent.SchemaRegistry.Schema;

namespace sample_stream_registry_chr_avro.Helpers
{
    internal class TopicBuilder
    {
        private readonly string _bootstrapServers;
        private readonly SchemaRegistryConfig _schemaRegistryConfig;
        private readonly SchemaBuilder _builder;
        private readonly JsonSchemaWriter _writer;

        public TopicBuilder(string bootstrapServers, string schemaRegistryUrl)
        {
            _bootstrapServers = bootstrapServers;
            _schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl
            };

            _builder = new SchemaBuilder();
            _writer = new JsonSchemaWriter();
        }

        public void CreateTopics(Dictionary<string, Type> topics)
        {
            try
            {
                topics.ToList().ForEach(async kvp => await CreateTopicWithSchema(kvp.Key, kvp.Value));
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"An error occurred creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
        }

        async Task CreateTopicWithSchema(string topic, Type topicType)
        {
            var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _bootstrapServers
            }).Build();

            if (adminClient.TopicExists(topic))
                return;

            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topic,
                    ReplicationFactor = 1,
                    NumPartitions = 1
                }
            });

            await SetAvroSchemaAsync(topic, topicType);
        }

        async Task SetAvroSchemaAsync(string topic, Type topicType)
        {
            var chrAvroSchema = _builder.BuildSchema(topicType);
            var schema = _writer.Write(chrAvroSchema);

            using var registry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            await registry.RegisterSchemaAsync(topic, new Schema(schema, SchemaType.Avro))
                .ConfigureAwait(false);
        }
    }
}
