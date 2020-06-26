using Confluent.Kafka;
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

            builder.Stream<string, string>("test")
                    .GroupByKey()
                    .WindowedBy(TimeWindowOptions.Of(TimeSpan.FromMinutes(1)))
                    .Count(InMemoryWindows<string, long>.As("store"))
                    .ToStream()
                    .Map((k, v) => KeyValuePair.Create(k.Key, v.ToString()))
                    .To("output");

            Topology t = builder.Build();
            bool taskStart = false;

            KafkaStream stream = new KafkaStream(t, config);

            // Subscribe state changed
            //stream.StateChanged += (old, @new) =>
            //{
            //    if (!taskStart && @new == KafkaStream.State.RUNNING) // If new state is running, we can quering state store.
            //    {
            //        Task.Factory.StartNew(() =>
            //        {
            //            while (!source.Token.IsCancellationRequested)
            //            {
            //                var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.WindowStore<string, long>()));
            //                var items = store.All().ToList();
            //                Thread.Sleep(500);
            //            }
            //        }, source.Token);
            //        taskStart = true;
            //    }
            //};

            Console.CancelKeyPress += (o, e) =>
            {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }
    }
}
