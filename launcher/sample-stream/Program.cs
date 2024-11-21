using Streamiz.Kafka.Net;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
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
                Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Information);
                }),
                NumStreamThreads = 2
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
            
            builder
                .Stream<string, string>("input")
                .Map((k, v, r) =>
                {
                    var newKey = Guid.NewGuid().ToString();
                    return KeyValuePair.Create(newKey, newKey);
                })
                .GroupByKey()
                .Aggregate(
                    () => Guid.NewGuid().ToString(),
                    (key, curr, acc) => acc,
                        RocksDb.As<string, string, StringSerDes, StringSerDes>("test-ktable")
                )
                .ToStream()
                            .To("output");
                        
            return builder.Build();
        }
    }
}
