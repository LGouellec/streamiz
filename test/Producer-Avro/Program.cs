using com.avro.bean;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;

namespace Producer_Avro
{
    internal class Program
    {
        private static ProducerConfig producerConfig = new ProducerConfig
        {
            Acks = Acks.All,
            BootstrapServers = "192.168.56.1:9092",
            SaslMechanism = SaslMechanism.Plain,
            SaslPassword = "admin",
            SaslUsername = "admin",
            SecurityProtocol = SecurityProtocol.SaslPlaintext
        };

        private static CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient
                (new SchemaRegistryConfig
                {
                    Url = "http://192.168.56.1:8081"
                });

        private static void Main(string[] args)
        {
            var topic = args.Length > 0 ? args[0] : "orders";

            if (topic.Equals("orders"))
            {
                ProduceOrder();
            }
            else if (topic.Equals("product"))
            {
                ProduceProduct();
            }
        }

        static void ProduceOrder()
        {
            var builder = new ProducerBuilder<int, Order>(producerConfig)
                .SetValueSerializer(new AvroSerializer<Order>(schemaRegistryClient, new AvroSerializerConfig { AutoRegisterSchemas = true }).AsSyncOverAsync());
            Console.WriteLine($"Writting in orders topic");
            Console.WriteLine("Enter exit for stopping producer, or enter PRODUCT_ID:ORDER_ID|PRICE");
            using (var producer = builder.Build())
            {
                string s = Console.ReadLine();
                while (!s.Contains("exit", StringComparison.InvariantCultureIgnoreCase))
                {
                    string[] r = s.Split(":");
                    string[] p = r[1].Split("|");
                    producer.Produce("orders", new Message<int, Order>
                    {
                        Key = int.Parse(r[0]),
                        Value = new Order
                        {
                            order_id = int.Parse(p[0]),
                            price = float.Parse(p[1]),
                            product_id = int.Parse(r[0])
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

        static void ProduceProduct()
        {
            var builder = new ProducerBuilder<int, Product>(producerConfig)
                  .SetValueSerializer(new AvroSerializer<Product>(schemaRegistryClient, new AvroSerializerConfig { AutoRegisterSchemas = true }).AsSyncOverAsync());
            Console.WriteLine($"Writting in product topic");
            Console.WriteLine("Enter exit for stopping producer, or enter PRODUCT_ID:PRODUCT_NAME|PRICE");
            using (var producer = builder.Build())
            {
                string s = Console.ReadLine();
                while (!s.Contains("exit", StringComparison.InvariantCultureIgnoreCase))
                {
                    string[] r = s.Split(":");
                    string[] p = r[1].Split("|");
                    producer.Produce("product", new Message<int, Product>
                    {
                        Key = int.Parse(r[0]),
                        Value = new Product
                        {
                            name = p[0],
                            price = float.Parse(p[1]),
                            product_id = int.Parse(r[0])
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
