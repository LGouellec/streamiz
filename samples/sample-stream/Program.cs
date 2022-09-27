using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var config1 = new StreamConfig<StringSerDes, StringSerDes>();
            config1.ApplicationId = "app-count-word";
            config1.BootstrapServers = "localhost:9092";
            config1.AutoOffsetReset = AutoOffsetReset.Earliest;
            config1.StateDir = Path.Combine("/tmp/state");
            config1.CommitIntervalMs = 5000;
            config1.Logger = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Debug);
                builder.AddLog4Net();
            });
            
            StreamBuilder builder = new StreamBuilder();
            
            builder.Stream<string, string>("words")
                .FlatMapValues((k, v) => v.Split(" "))
                .SelectKey((k, v) => v)
                .GroupByKey()
                .Count(
                    RocksDb<string, long>
                        .As("count-store")
                        .WithKeySerdes(new StringSerDes())
                        .WithValueSerdes(new Int64SerDes()))
                .ToStream()
                .Print(Printed<string, long>.ToOut());
            
            var topo = builder.Build();
            KafkaStream stream = new KafkaStream(topo, config1);

            Console.CancelKeyPress += (o,e) => stream.Dispose();
            
            await stream.StartAsync();
        }
    }
}