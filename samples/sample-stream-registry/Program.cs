using com.avro.bean;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace sample_stream_registry
{
    class Program
    {
        static async Task Main(string[] args)
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
            // NEED FOR SchemaAvroSerDes
            config.SchemaRegistryUrl = "http://192.168.56.1:8081";
            config.AutoRegisterSchemas = false;

            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, Person, StringSerDes, SchemaAvroSerDes<Person>>("person")
                    .Filter((k, v) => v.age >= 18)
                    .MapValues((v) => $"{v.firstName}-{v.lastName}-{v.age}")
                    .To<StringSerDes, StringSerDes>("output");

            Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) =>
            {
                stream.Dispose();
            };

            await stream.StartAsync();
        }
    }
}
