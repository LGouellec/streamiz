using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Threading;

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
                            .Stream<string, string>("test")
                            .GroupByKey()
                            .WindowedBy(TimeWindowOptions.Of(TimeSpan.FromSeconds(2)))
                            .Count(InMemoryWindows<string, long>.As("count-store", TimeSpan.FromSeconds(2)));

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                inputTopic.PipeInput("renault", "clio");
                inputTopic.PipeInput("renault", "megane");
                Thread.Sleep(2000);
                inputTopic.PipeInput("renault", "scenic");
                var store = driver.GetKeyValueStore<string, long>("count-store");
                var elements = store.All();
            }
        }
    }
}
