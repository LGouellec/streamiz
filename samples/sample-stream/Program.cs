using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Stream;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        private class OrderShoe
        {
            private readonly Order order;
            private readonly Shoe shoe;

            public OrderShoe(Program.Order order, Program.Shoe shoe)
            {
                this.order = order;
                this.shoe = shoe;
            }
        }

        private class Shoe
        {
            public string id { get; set; }
            public string brand { get; set; }
            public string name { get; set; }
            public int sale_price { get; set; }
            public float rating { get; set; }
        }

        private class Order
        {
            public int order_id { get; set; }
            public string product_id { get; set; }
            public string customer_id { get; set; }
            public long ts { get; set; }
        }
        
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-app-reproducer",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                CommitIntervalMs = 3000,
                StateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()),
                Logger = LoggerFactory.Create((b) =>
                {
                    b.SetMinimumLevel(LogLevel.Error);
                    b.AddLog4Net();
                })
            };

            var builder = new StreamBuilder();

            var shoes = builder.Stream<String, Shoe, StringSerDes, JsonSerDes<Shoe>>("shoes");
            var orders = builder.Stream<String, Order, StringSerDes, JsonSerDes<Order>>("orders");

            orders
                .SelectKey((k, v) => v.product_id.ToString())
                .Join<Shoe, OrderShoe, JsonSerDes<Shoe>, JsonSerDes<OrderShoe>>(
                    shoes,
                    (order, shoe) => new OrderShoe(order, shoe),
                    JoinWindowOptions.Of(TimeSpan.FromHours(1)))
                .To<StringSerDes, JsonSerDes<OrderShoe>>("order-shoes");
            
            Topology t = builder.Build();
            KafkaStream stream1 = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream1.Dispose();
            
            await stream1.StartAsync();
        }
    }
    
}