using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamToTableTests
    {
        [Test]
        public void TestToTableEmpty()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-to-table"
            };

            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("test")
                .Filter((k, v) => v.Length % 2 == 0)
                .ToTable()
                .ToStream()
                .To("output");


            List<KeyValuePair<string, string>> expected = new List<KeyValuePair<string, string>>
            {
                KeyValuePair.Create("test", "test"),
                KeyValuePair.Create("test", "1234")
            };

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "test");
                inputTopic.PipeInput("test", "tes");
                inputTopic.PipeInput("test", "1234");
                var elements = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();
                Assert.AreEqual(expected, elements);
            }

        }
        
        [Test]
        public void TestToTable()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-to-table"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                .Stream<string, string>("test")
                .Filter((k, v) => v.Length % 2 == 0)
                .ToTable(InMemory.As<string, string>("table-store"));

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                inputTopic.PipeInput("test", "test");
                inputTopic.PipeInput("test", "tes");
                var store = driver.GetKeyValueStore<string, string>("table-store");
                Assert.IsNotNull(store);
                Assert.AreEqual("test", store.Get("test"));
                inputTopic.PipeInput("test", "test12");
                Assert.AreEqual("test12", store.Get("test"));

            }
        }
    }
}
