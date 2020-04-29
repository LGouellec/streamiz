using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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

            builder.Stream<string, string>("test")
                .FilterNot((k, v) => v.Contains("test"))
                .To("test-output");

            builder.Table("test-ktable", InMemory<string, string>.As("test-store"));

            Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);

            stream.StateChanged += (old, @new) =>
            {
                if (@new == KafkaStream.State.RUNNING)
                {
                    Task.Factory.StartNew(() =>
                    {
                        while (!source.Token.IsCancellationRequested)
                        {
                            var store = stream.Store("test-store", QueryableStoreTypes.TimestampedKeyValueStore<string, string>());
                            var items = store.All().ToList();
                            var store2 = stream.Store("test-store", QueryableStoreTypes.KeyValueStore<string, string>());
                            var items2 = store2.All().ToList();
                            Thread.Sleep(500);
                        }
                    }, source.Token);
                }
            };
            Console.CancelKeyPress += (o, e) =>
            {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }
    }
}
