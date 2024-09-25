using Streamiz.Kafka.Net;
using System;
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
            var rocksDbHandler = new BoundMemoryRocksDbConfigHandler()
                .ConfigureNumThreads(2)
                .SetCompactionStyle(Compaction.Universal)
                .SetCompressionType(Compression.Lz4)
                .LimitTotalMemory(CacheSize.OfMb(40));
            
           var config = new StreamConfig<StringSerDes, StringSerDes>{
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Logger = LoggerFactory.Create(b =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Information);
                }),
                RocksDbConfigHandler = rocksDbHandler.Handle
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

            /*builder.Stream<string, string>("input")
                .DropDuplicate((key, value1, value2) => value1.Equals(value2),
                    TimeSpan.FromMinutes(1))
                .To(
                    "output");*/

            builder.Stream<string, string>("users")
                .GroupByKey()
                .Count()
                .ToStream("count_users");
            
            return builder.Build();
        }
    }
}
