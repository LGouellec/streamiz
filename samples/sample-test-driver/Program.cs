using Confluent.Kafka;
using kafka_stream_core;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.Mock;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using System;

namespace sample_test_driver
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-test-driver-app";
            
            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, string>("test")
                .Filter((k, v) => v.Contains("test"))
                .To("test-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(5));
                inputTopic.PipeInput("test", "coucou");
                inputTopic.PipeInput("test", "test-coucou");
                var r = outputTopic.ReadKeyValueList();
            }
        }
    }
}
