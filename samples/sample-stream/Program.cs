using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.Stream;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    public static class Program
    {
        public static async Task Main2(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>{
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Debug);
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
            TimeSpan windowSize = TimeSpan.FromHours(1);
            
            var builder = new StreamBuilder();
            /*builder.Stream<string, string>("input")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(windowSize))
                .Count(RocksDbWindows.As<string, long>("count-store")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes())
                    .WithCachingEnabled())
                .ToStream()
                .Map((k,v) => new KeyValuePair<string,string>(k.ToString(), v.ToString()))
                .To("output",
                    new StringSerDes(),
                    new StringSerDes());*/

            builder.Stream<string, string>("input")
                .DropDuplicate((key, value1, value2) => value1.Equals(value2),
                    TimeSpan.FromMinutes(1))
                .To(
                    "output");//, (s, s1, arg3, arg4) => new Partition(0));

            
            return builder.Build();
        }
    }
}