using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Security.Permissions;
using System.Text.Json;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes.CloudEvents;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig();
            config.ApplicationId = "test-app-218";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 3000;
            config.StateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Information);
                b.AddLog4Net();
            });
            config.UsePrometheusReporter(9090);
            config.MetricsRecording = MetricsRecordingLevel.DEBUG;

            StreamBuilder builder = new StreamBuilder();

            string inputTopic = "words", outputTopic = "words-count";
            TimeSpan windowSize = TimeSpan.FromDays(1);
            
            IKStream<string, string> stream = builder.Stream<string, string>(inputTopic);
            stream
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(windowSize))
                .Count()
                .ToStream()
                .Map((k,v) => KeyValuePair.Create(k.Key, v))
                .To<StringSerDes, Int64SerDes>(outputTopic);

            stream
                .GroupByKey()
                .Count(
                    InMemory.As<string, long>("count-store")
                        .WithKeySerdes<StringSerDes>()
                        .WithValueSerdes<Int64SerDes>());
            
            Topology t = builder.Build();
            KafkaStream stream1 = new KafkaStream(t, config);

            Console.CancelKeyPress += (_, _) => stream1.Dispose();
            
            await stream1.StartAsync();
        }
    }
}