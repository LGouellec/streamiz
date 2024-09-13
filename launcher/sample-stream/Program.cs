using Streamiz.Kafka.Net;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.SerDes;
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
                Guarantee = ProcessingGuarantee.AT_LEAST_ONCE,
                StateDir = ".",
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Information);
                })
            };
           
            config.MetricsRecording = MetricsRecordingLevel.DEBUG;
            config.UsePrometheusReporter(9090, true);
                   
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
            
            builder.Stream<string, string>("input3")
                .Peek((k,v,c) => Console.WriteLine($"Key : {k} Context : {c.Topic}:{c.Partition}:{c.Offset}"))
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .Count(RocksDbWindows.As<string, long>("count-store")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes()))
                    //.WithCachingEnabled()
                .ToStream()
                .Map((k,v, _) => new KeyValuePair<string,string>(k.ToString(), v.ToString()))
                .To("output3",
                    new StringSerDes(),
                    new StringSerDes());

            /*builder.Stream<string, string>("input3")
                .Peek((k,v,c) => Console.WriteLine($"Key : {k} Context : {c.Topic}:{c.Partition}:{c.Offset}"))
                .To("output3",
                    new StringSerDes(),
                    new StringSerDes());


            builder.Stream<string, string>("input")
                .DropDuplicate((key, value1, value2) => value1.Equals(value2),
                    TimeSpan.FromMinutes(1))
                .To(
                    "output");//, (s, s1, arg3, arg4) => new Partition(0));*/
            
            return builder.Build();
        }
    }
}
