using com.avro.bean;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;

namespace Producer
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var producerConfig = new ProducerConfig
            {
                Acks = Acks.All,
                BootstrapServers = "192.168.56.1:9092",
                SaslMechanism = SaslMechanism.Plain,
                SaslPassword = "admin",
                SaslUsername = "admin",
                SecurityProtocol = SecurityProtocol.SaslPlaintext
            };
            var topic = args.Length > 0 ? args[0] : "person";
            var schemaRegistryClient = new CachedSchemaRegistryClient
                (new SchemaRegistryConfig
                {
                    Url = "http://192.168.56.1:8081"
                });
            var builder = new ProducerBuilder<string, Person>(producerConfig)
                .SetValueSerializer(new AvroSerializer<Person>(schemaRegistryClient).AsSyncOverAsync());
            Console.WriteLine($"Writting in {topic} topic");
            Console.WriteLine("Enter exit for stopping producer, or enter KEY:VALUE");
            using (var producer = builder.Build())
            {
                string s = Console.ReadLine();
                while (!s.Contains("exit", StringComparison.InvariantCultureIgnoreCase))
                {
                    string[] r = s.Split(":");
                    string[] p = r[1].Split("|");
                    producer.Produce(topic, new Message<string, Person>
                    {
                        Key = r[0],
                        Value = new Person
                        {
                            age = Convert.ToInt32(p[2]),
                            lastName = p[0],
                            firstName = p[1]
                        }
                    }, (d) =>
                    {
                        if (d.Status == PersistenceStatus.Persisted)
                        {
                            Console.WriteLine("Message sent !");
                        }
                    });
                    s = Console.ReadLine();
                }
            }
        }
    }
}
