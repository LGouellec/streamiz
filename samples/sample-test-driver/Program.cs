using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;

namespace sample_test_driver
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-test-driver-app"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Table<string, string>("test")
                            .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
                            .Count(InMemory<string, long>.As("count-store"));

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                inputTopic.PipeInput("renault", "clio");
                inputTopic.PipeInput("renault", "megane");
                inputTopic.PipeInput("ferrari", "red");
                var store = driver.GetKeyValueStore<string, long>("count-store");
                var elements = store.All();
            }
        }
    }
}
