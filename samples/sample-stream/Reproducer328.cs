using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace sample_stream;

public class Reproducer328
{
    public class Product
    {
        public string category { get; set; }
        public string name { get; set; }
        public string description { get; set; }
        public double price { get; set; }

        public override string ToString()
        {
            return $"Name:{name}, Category: {category}, Price: {price}";
        }
    }
    
    public static async Task Main(string[] args)
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = $"test-reproducer328",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            MaxPollRecords = 1,
            Logger = LoggerFactory.Create(b =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddLog4Net();
            })
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

        builder.Stream<string, Product, StringSerDes, JsonSerDes<Product>>("product3")
            .Peek((k, v) => Console.WriteLine($"Product Key : {k} | Product Value : {v}"))
            .Join(global,
                (s, s1) => s1.category,
                (product, s) =>
                {
                    product.category = s;
                    return product;
                })
            .Peek((k, v) => Console.WriteLine($"Product Key : {k} | Product Value : {v}"))
            .To<StringSerDes, JsonSerDes<Product>>("product_output3");
            
        return builder;
    }
}