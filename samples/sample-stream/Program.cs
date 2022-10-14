using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;
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
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "app-count-word";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.StateDir = Path.Combine("/tmp/state");
            config.CommitIntervalMs = 5000;
            config.Logger = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Debug);
                builder.AddLog4Net();
            });
            config.MetricsRecording = MetricsRecordingLevel.DEBUG;
            config.UsePrometheusReporter(9090);

            StreamBuilder builder = new StreamBuilder();
            builder.Stream<string, string>("words")
                .FlatMapValues((v) => v.Split(" "))
                .SelectKey((k, v) => v)
                .GroupByKey()
                .Count(RocksDb.As<string, long, StringSerDes, Int64SerDes>("count-store"));

            var topo = builder.Build();
            
            KafkaStream stream = new KafkaStream(topo, config);

            Console.CancelKeyPress += (o,e) => stream.Dispose();
            
            await stream.StartAsync();
        }
    }
}