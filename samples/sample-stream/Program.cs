using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Threading;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.State;

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
            config.ApplicationId = "test-app2";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.StateDir = Path.Combine(".");
            config.CommitIntervalMs = 5000;
            config.Logger = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddLog4Net();
            });

            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("input")
                .ExternalCallAsync(
                    async (record, context) =>
                        await Task.FromResult(new KeyValuePair<string, string>(record.Key, record.Value)),
                    RetryPolicyBuilder
                        .NewBuilder()
                        .NumberOfRetry(10)
                        .RetryBackOffMs(100)
                        .Build())
                .MapValues((k,v) => v.ToUpper())
                .To("output");

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (o, e) => stream.Dispose();

            await stream.StartAsync();
        }
    }
}