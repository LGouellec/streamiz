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
                            .Stream<string, string>("test")
                            .GroupByKey()
                            .Reduce((v1, v2) => v2.Length > v1.Length ? v2 : v1);

            var stream = builder
                            .Stream<string, string>("stream")
                            .Join<string, string, StringSerDes, StringSerDes>(table, (s, v) => $"{s}-{v}");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var inputTopic2 = driver.CreateInputTopic<string, string>("stream");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
            }
        }
    }
}
