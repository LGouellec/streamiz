using Confluent.Kafka;
using Microsoft.VisualBasic;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace sample_stream
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-perf-app";
            config.BootstrapServers = "localhost:9093";
            config.PollMs = 100;
            config.MaxPollRecords = 500;
            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("test")
                .To("test-output");

            Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                stream.Dispose();
            };

            await stream.StartAsync();
        }
    }
}
