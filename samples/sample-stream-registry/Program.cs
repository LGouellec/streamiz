using com.avro.bean;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace sample_stream_registry
{
    class Program
    {
        static async Task Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();

            var config = new StreamConfig();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "localhost:9092";
            // NEED FOR SchemaAvroSerDes
            config.SchemaRegistryUrl = "http://localhost:8081";
            config.AutoRegisterSchemas = true;

            StreamBuilder builder = new StreamBuilder();

            var table = builder.Table("product",
                                new Int32SerDes(),
                                new SchemaAvroSerDes<Product>(),
                                InMemory<int, Product>.As("product-store"));

            var orders = builder.Stream<int, Order, Int32SerDes, SchemaAvroSerDes<Order>>("orders");
            
            orders.Join(table, (order, product) => new OrderProduct
                    {
                        order_id = order.order_id,
                        price = order.price,
                        product_id = product.product_id,
                        product_name = product.name,
                        product_price = product.price
                    })
                    .To<Int32SerDes, SchemaAvroSerDes<OrderProduct>>("orders-output");

            orders
                .GroupByKey()
                .Aggregate<OrderAgg, SchemaAvroSerDes<OrderAgg>>(
                    () => new OrderAgg(),
                    (key, order, agg) =>
                    {
                        agg.order_id = order.order_id;
                        agg.price = order.price;
                        agg.product_id = order.product_id;
                        agg.totalPrice += order.price;
                        return agg;
                    })
                .ToStream()
                .Print(Printed<int, OrderAgg>.ToOut());

                Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) =>
            {
                stream.Dispose();
            };

            await stream.StartAsync();
        }
    }
}
