using kafka_stream_core;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using kafka_stream_core.Table;
using System;

namespace sample_stream
{
    class Program
    {
        static void Main(string[] args)
        {
            StreamConfig config = new StreamConfig();
            config.ApplicationId = "test-app";
            config.Add("bootstrap.servers", "192.168.56.1:9092");
            config.Add("sasl.mechanism", "SCRAM-SHA-512");
            config.Add("sasl.username", "admin");
            config.Add("sasl.password", "admin");
            config.Add("security.protocol", "SaslPlaintext");
            config.NumStreamThreads = 2;

            StreamBuilder builder = new StreamBuilder();
            builder.stream("test").filterNot((k, v) => v.Contains("test")).to("test-output");
            var ktable = builder.table("test-ktable", Consumed<string, string>.with(new StringSerDes(), new StringSerDes()), InMemory<string, string>.As("test-ktable-store"));

            Topology t = builder.build();
            KafkaStream stream = new KafkaStream(t, config);

            try
            {
                stream.Start();
                Console.ReadKey();
                stream.Stop();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + ":" + e.StackTrace);
                stream.Kill();
            }
        }
    }
}
