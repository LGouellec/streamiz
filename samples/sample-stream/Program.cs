using Streamiz.Kafka.Net;
using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Azure.RemoteStorage;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
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
                    b.SetMinimumLevel(LogLevel.Information);
                })
            };
           
            config["azure.remote.storage.uri"] = "URI";
            config["azure.remote.storage.account.name"] = "ACCOUNT_NAME";
            config["azure.remote.storage.account.key"] = "MASTER_KEY";
            
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

            builder.Stream<string, string>("input")
                .DropDuplicate((key, value1, value2) => value1.Equals(value2),
                    TimeSpan.FromMinutes(1))
                .To(
                    "output");//, (s, s1, arg3, arg4) => new Partition(0));
            
            return builder.Build();
        }
    }
}