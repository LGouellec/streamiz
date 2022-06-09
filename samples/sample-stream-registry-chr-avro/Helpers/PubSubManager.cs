using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Newtonsoft.Json;

namespace sample_stream_registry_chr_avro.Helpers
{
    internal class PubSubManager
    {
        private readonly string _bootstrapServers;
        private readonly SchemaRegistryConfig _schemaRegistryConfig;

        public PubSubManager(string bootstrapServers, string schemaRegistryUrl)
        {
            _bootstrapServers = bootstrapServers;
            _schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl
            };
        }

        public async Task ProduceAsync<T>(string topic, string key, T message) where T : class
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers
            };

            using var registry = new CachedSchemaRegistryClient(_schemaRegistryConfig);

            var builder = new ProducerBuilder<string, T>(producerConfig)
                .SetAvroValueSerializer(registry, AutomaticRegistrationBehavior.Always)
                .SetErrorHandler((_, error) => Console.Error.WriteLine(error.ToString()));

            using var producer = builder.Build();
            await producer.ProduceAsync(topic, new Message<string, T>
            {
                Key = key,
                Value = message
            });

            Console.WriteLine($"Published message with key {key} and value {JsonConvert.SerializeObject(message)}");
        }

        public void Consume<T>(string topic) where T : class
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "example_consumer_group"
            };

            using var registry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            var builder = new ConsumerBuilder<string, T>(consumerConfig)
                .SetAvroValueDeserializer(registry)
                .SetErrorHandler((_, error) => Console.Error.WriteLine(error.ToString()));

            using var consumer = builder.Build();
            consumer.Subscribe(topic);

            while (true)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result?.Message != null && !result.IsPartitionEOF)
                {
                    Console.WriteLine(
                        $"Received message for topic {topic} with key {result.Message.Key} and value {JsonConvert.SerializeObject(result.Message.Value)}");
                }
            }
        }
    }
}
