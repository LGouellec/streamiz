using Confluent.Kafka;
using Microsoft.VisualBasic;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
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
        private static void Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "192.168.56.1:9092";
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = "admin";
            config.SaslPassword = "admin";
            config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.NumStreamThreads = 1;

            StreamBuilder builder = new StreamBuilder();

            var stream1 = builder.Stream<string, string>("test");
            var stream2 = builder.Stream<string, string>("test2");

            stream1.Join<string, string, StringSerDes, StringSerDes>(stream2,
                (v1, v2) => $"{v1}-{v2}",
                JoinWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .To("output-join");


            Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);


            Console.CancelKeyPress += (o, e) =>
            {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }
    }
}
