using sample_stream_registry_chr_avro.Helpers;
using sample_stream_registry_chr_avro.Models;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Chr.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream_registry_chr_avro
{
    public class Program
    {
        private const string _productTopic = "product";
        private const string _ordersTopic = "orders";
        private const string _ordersOutputTopic = "orders-output";
        
        static async Task Main()
        {
            var config = new StreamConfig
            {
                ApplicationId = "test-app",
                BootstrapServers = "localhost:9092",
                //BootstrapServers = "broker:29092",
                SchemaRegistryUrl = "http://localhost:8081",
                //SchemaRegistryUrl = "http://schema-registry:8081",
                AutoRegisterSchemas = true,
                AllowAutoCreateTopics = false
            };

            var topicBuilder = new TopicBuilder(config.BootstrapServers, config.SchemaRegistryUrl);

            var topicsWithTypes = new Dictionary<string, Type>
            {
                { _productTopic, typeof(Product) },
                { _ordersTopic, typeof(Order) },
                { _ordersOutputTopic, typeof(OrderProduct) }
            };

            topicBuilder.CreateTopics(topicsWithTypes);

            var topology = BuildTopology();

            var stream = new KafkaStream(topology, config);

            Console.CancelKeyPress += (_, _) => stream.Dispose();

            await stream.StartAsync();

            await RunJoinExample(config.BootstrapServers, config.SchemaRegistryUrl);
        }

        private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();

            var table = builder.Table(_productTopic,
                new Int32SerDes(),
                new HostedSchemaAvroSerDes<Product>(),
                InMemory<int, Product>.As("product-store"));

            var orders = builder.Stream<int, Order, Int32SerDes, HostedSchemaAvroSerDes<Order>>(_ordersTopic);

            orders.Join(table, (order, product) => new OrderProduct
            {
                OrderId = order.OrderId,
                Price = order.Price,
                ProductId = product.ProductId,
                ProductName = product.Name,
                ProductPrice = product.Price
            })
                .To<Int32SerDes, HostedSchemaAvroSerDes<OrderProduct>>(_ordersOutputTopic);

            orders
                .GroupByKey()
                .Aggregate<OrderAgg, HostedSchemaAvroSerDes<OrderAgg>>(
                    () => new OrderAgg(),
                    (_, order, agg) =>
                    {
                        agg.OrderId = order.OrderId;
                        agg.Price = order.Price;
                        agg.ProductId = order.ProductId;
                        agg.TotalPrice += order.Price;
                        return agg;
                    })
                .ToStream()
                .Print(Printed<int, OrderAgg>.ToOut());

            return builder.Build();
        }

        private static async Task RunJoinExample(string bootstrapServers, string schemaRegistryUrl)
        {
            var pubSubManager = new PubSubManager(bootstrapServers, schemaRegistryUrl);

            var key = Guid.NewGuid().ToString();
            var product = new Product
            {
                Name = "dummy-product-name",
                Price = 12.5f,
                ProductId = 5
            };

            await pubSubManager.ProduceAsync(_productTopic, key, product);

            var order = new Order
            {
                OrderId = 6,
                Price = 5f,
                ProductId = 5
            };

            await pubSubManager.ProduceAsync(_ordersTopic, key, order);

            pubSubManager.Consume<OrderProduct>(_ordersOutputTopic);
        }
    }
}
