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
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-test-driver-app";

            StreamBuilder builder = new StreamBuilder();

            var stream = builder
               .Stream<string, string>("test")
               .GroupBy((k, v) => k.ToUpper());

            stream.Count(InMemory<string, long>.As("count-store"));
            stream.Aggregate(
                    () => new Dictionary<char, int>(),
                    (k, v, old) =>
                    {
                        var caracs = v.ToCharArray();
                        foreach(var c in caracs)
                        {
                            if (old.ContainsKey(c))
                                ++old[c];
                            else
                                old.Add(c, 1);
                        }
                        return old;
                    },
                    InMemory<string, Dictionary<char, int>>.As("agg-store").WithValueSerdes(new DictionarySerDes())
                );
            stream.Reduce((v1, v2) => v2.Length > v1.Length ? v2 : v1, InMemory<string, string>.As("reduce-store"));

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(5));
                inputTopic.PipeInput("test", "test");
                inputTopic.PipeInput("test", "test2");
                var store = driver.GetKeyValueStore<string, string>("reduce-store");
                var e = store.Get("TEST");
                //var store = driver.GetKeyValueStore<string, Dictionary<char, int>>("agg-store");
                //var el = store.Get("TEST");
                //var storeCount = driver.GetKeyValueStore<string, long>("count-store");
                //var e = storeCount.Get("TEST");
            }
        }
    }
}
