using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;

namespace sample_test_driver
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-test-driver-app";

            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("test")
                .GroupBy((k, v) => k.ToUpper())
                .Count(InMemory<string, long>.As("count-store"));

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(5));
                inputTopic.PipeInput("test", "test");
                inputTopic.PipeInput("test", "test2");
                var store = driver.GetKeyValueStore<string, long>("count-store");
                var el = store.Get("TEST"); // SOULD EQUAL 2
            }
        }
    }
}
