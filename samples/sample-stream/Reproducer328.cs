using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace sample_stream;

public class Reproducer328
{
    public class Product
    {
        public string Category { get; set; }
        public string Name { get; set; }

        public override string ToString()
        {
            return $"Name:{Name}, Category: {Category}";
        }
    }
    
    public static async Task Main(string[] args)
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = $"test-reproducer328",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var builder = CreateTopology();
        var t = builder.Build();
        var stream = new KafkaStream(t, config);

        Console.CancelKeyPress += (sender, eventArgs) => stream.Dispose();
        
        await stream.StartAsync();
    }

    private static StreamBuilder CreateTopology()
    {
        StreamBuilder builder = new StreamBuilder();
        var global = builder.GlobalTable<string, string>("product_category",
            InMemory.As<string, string>("product-store"));

        builder.Stream<string, Product, StringSerDes, JsonSerDes<Product>>("product")
            .Peek((k, v) => Console.WriteLine($"Product Key : {k} | Product Value : {v}"))
            .Join(global,
                (s, s1) => s1.Category,
                (product, s) =>
                {
                    product.Category = s;
                    return product;
                })
            .Peek((k, v) => Console.WriteLine($"Product Key : {k} | Product Value : {v}"))
            .To<StringSerDes, JsonSerDes<Product>>("product_output");
            
        return builder;
    }
}