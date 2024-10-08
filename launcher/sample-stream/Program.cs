using Streamiz.Kafka.Net;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    public static class Program
    {
       public static async Task Main(string[] args)
       {
           var config = new StreamConfig<StringSerDes, StringSerDes>{
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Information);
                })
            };
           
            var t = BuildTopology();
            var stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_,_) => {
                stream.Dispose();
            };

            await stream.StartAsync();
        }
        
        private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();
            
            builder.Stream<string, string>("input")
                .GroupByKey()
                .Count()
                .Suppress(SuppressedBuilder.UntilTimeLimit<string>(TimeSpan.FromMinutes(1), StrictBufferConfig.Unbounded()))
                .ToStream()
                .To("output");
            
            return builder.Build();
        }
    }
}
