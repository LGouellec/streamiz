using System;
using System.Collections.Generic;
using kafka_stream_core;
using kafka_stream_core.Nodes.Parameters;
using kafka_stream_core.SerDes;

namespace test_stream
{
    class Program
    {
        static void Main(string[] args)
        {
            Configuration config = new Configuration();
            config.ApplicationId = "test-app";
            config.Add("bootstrap.servers", "192.168.56.1:9092");
            config.Add("sasl.mechanism", "SCRAM-SHA-512");
            config.Add("sasl.username", "admin");
            config.Add("sasl.password", "Michelin/1");
            config.Add("security.protocol", "SaslPlaintext");

            StreamBuilder builder = new StreamBuilder();
            builder.stream("test")
                .filterNot((k, v) => v.Contains("toto"))
                .transform((k,v) => new KeyValuePair<string, string>(k, v.ToUpper()))
                .to("test2");

            Topology t = builder.build();
            KafkaStream stream = new KafkaStream(t, config);
            try
            {
                stream.start();
                Console.ReadKey();
                stream.stop();
            }catch(Exception e)
            {
                Console.WriteLine(e.Message + ":" + e.StackTrace);
                stream.kill();
            }
        }
    }
}
