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
        private static ProducerConfig producerConfig = new ProducerConfig
        {
            Acks = Acks.All,
            BootstrapServers = "localhost:9092"
        };

        private static CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient
                (new SchemaRegistryConfig
                {
                    Url = "http://localhost:8081"
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
                        Key = Int32.Parse(r[0]),
                        Value = new Order
                        {
                            order_id = Int32.Parse(p[0]),
                            price = float.Parse(p[1]),
                            product_id = Int32.Parse(r[0])
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
                        Key = Int32.Parse(r[0]),
                        Value = new Product
                        {
                            name = p[0],
                            price = float.Parse(p[1]),
                            product_id = Int32.Parse(r[0])
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
