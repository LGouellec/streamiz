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
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "localhost:29092";
            config.NumStreamThreads = 3;
            config.PollMs = 10;
            config.BufferedRecordsPerPartition = 20000;

            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, string>("test")
                    .Filter((k, v) => v.Length % 2 == 0)
                    .To("test-output");

            Topology t = builder.Build();

            KafkaStream stream1 = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                stream1.Dispose();
            };

            await stream1.StartAsync();
        }
    }
}
