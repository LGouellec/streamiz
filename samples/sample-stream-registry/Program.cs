using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Threading;

namespace sample_stream_registry
{
    class Program
    {
        static void Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();

            var config = new StreamConfig();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "192.168.56.1:9092";
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = "admin";
            config.SaslPassword = "admin";
            config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

            StreamBuilder builder = new StreamBuilder();
            var personSerDes = new PersonSerDes(new SchemaRegistryConfig
            {
                Url = "http://192.168.56.1:8081"
            });

            builder.Stream("person", new StringSerDes(), personSerDes)
                    .Filter((k, v) => v.age >= 18)
                    .MapValues((v) => $"{v.firstName}-{v.lastName}-{v.age}")
                    .To<StringSerDes, StringSerDes>("output");

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
