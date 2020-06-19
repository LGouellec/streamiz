using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
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
                            .WindowedBy(TimeWindowOptions.Of(TimeSpan.FromSeconds(10)))
                            .Count(InMemoryWindows<string, long>.As("count-store"));

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                DateTime dt = DateTime.Now;
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                inputTopic.PipeInput("renault", "clio", dt);
                inputTopic.PipeInput("renault", "megane", dt.AddMilliseconds(10));
                Thread.Sleep((int)TimeSpan.FromSeconds(10).TotalMilliseconds);
                inputTopic.PipeInput("renault", "scenic", dt.AddSeconds(1));
                var store = driver.GetWindowStore<string, long>("count-store");
                var elements = store.All().ToList();
            }
        }
    }
}