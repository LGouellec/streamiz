using kafka_stream_core;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using kafka_stream_core.Table;
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
            config.Add("bootstrap.servers", "192.168.56.1:9092");
            config.Add("sasl.mechanism", "Plain");
            config.Add("sasl.username", "admin");
            config.Add("sasl.password", "admin");
            config.Add("security.protocol", "SaslPlaintext");
            config.NumStreamThreads = 2;

            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, string>("test")
                .FilterNot((k, v) => v.Contains("test"))
                .To("test-output");

            builder.Table(
                "test-ktable",
                StreamOptions.Create(),
                InMemory<string, string>.As("test-ktable-store"));

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
