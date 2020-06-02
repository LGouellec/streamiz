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

            builder.GlobalTable("test", InMemory<string, string>.As("test-store"));

            Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);
            bool taskGetStateStoreRunning = false;

            stream.StateChanged += (old, @new) =>
            {
                if (@new == KafkaStream.State.RUNNING && !taskGetStateStoreRunning)
                {
                    Task.Factory.StartNew(() =>
                    {
                        taskGetStateStoreRunning = true;
                        while (!source.Token.IsCancellationRequested)
                        {
                            var store = stream.Store(StoreQueryParameters.FromNameAndType("test-store", QueryableStoreTypes.KeyValueStore<string, string>()));
                            var items = store.All().ToList();
                            if (items.Count > 0)
                                foreach (var e in items)
                                    Console.WriteLine($"Key:{e.Key}|Value:{e.Value}");

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
