using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    internal static class StringHelper
    {
        internal static readonly Random rd = new Random();

        internal static string SubRandom(this string str)
        {
            var offset = rd.Next(str.Length - 1);
            return str.Substring((int)offset, (int)str.Length - offset);
        }
    }
    
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {        
        public static async Task Main(string[] args)
        {
            // kafka-producer-perf-test --producer-props bootstrap.servers=broker:29092 --topic input --record-size 200 --throughput 500 --num-records 10000000000
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-app-reproducer-eos",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                CommitIntervalMs = 2000,
                Guarantee = ProcessingGuarantee.AT_LEAST_ONCE,
                MaxPollRecords = 500,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                StateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()),
                Logger = LoggerFactory.Create((b) =>
                {
                    b.SetMinimumLevel(LogLevel.Information);
                    b.AddLog4Net();
                })
            };
            
            var builder = new StreamBuilder();

            builder
                .Stream("input", new StringSerDes(), new StringSerDes())
                .SelectKey((k,v) => "1")
                .GroupByKey()
                .Count(
                    InMemory.As<string, Int64>()
                        .WithKeySerdes(new StringSerDes())
                        .WithValueSerdes(new Int64SerDes()))
                .ToStream()
                .MapValues((v) => v.ToString())
                .To("output", new StringSerDes(), new StringSerDes());

            Topology t = builder.Build();
            KafkaStream stream1 = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream1.Dispose();
            
            await stream1.StartAsync();
        }
    }
    
}