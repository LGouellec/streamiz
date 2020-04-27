using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Threading;

namespace sample_stream
{
    class Program
    {
        static void Main(string[] args)
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

            //builder.Stream<string, string>("test")
            //    .FilterNot((k, v) => v.Contains("test"))
            //    .To("test-output");

            builder.Table("test-ktable", InMemory<string, string>.As("test-store"));

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }
    }
}
