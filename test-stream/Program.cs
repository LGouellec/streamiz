using kafka_stream_core;
using kafka_stream_core.Stream;
using System;
using System.Collections.Generic;

namespace test_stream
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
            config.Add("sasl.password", "Michelin/1");
            config.Add("security.protocol", "SaslPlaintext");

            StreamBuilder builder = new StreamBuilder();
            var s = builder.stream("test");
            KStream<String, String>[] branchs = s.branch((k, v) =>
               {
                   return v.Length % 2 == 0;
               }, (k, v) =>
               {
                   return v.Length % 2 != 0;
               });
            branchs[0].to("test-pair");
            branchs[1].to("test-impair");

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
