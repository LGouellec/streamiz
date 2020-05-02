using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableToStreamTests
    {
        [Test]
        public void KTableToStreamWithTransformation()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", InMemory<string, string>.As("table-topic-store"))
                .ToStream((k, v) => v.ToUpper())
                .To("table-stream");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("table-stream");
                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create("A", "a"));
                expected.Add(KeyValuePair.Create("B", "b"));

                inputTopic.PipeInput("key1", "a");
                inputTopic.PipeInput("key2", "b");

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                var resultK1 = store.Get("key1");
                var resultK2 = store.Get("key2");

                Assert.AreEqual("a", resultK1);
                Assert.AreEqual("b", resultK2);

                var results = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value));
                Assert.AreEqual(expected, results);
            }
        }

        [Test]
        public void KTableToStreamWithUpdate()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", InMemory<string, string>.As("table-topic-store"))
                .ToStream().To("table-stream");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("table-stream");
                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create("key1", "a"));
                expected.Add(KeyValuePair.Create("key2", "b"));
                expected.Add(KeyValuePair.Create("key2", "c"));

                inputTopic.PipeInput("key1", "a");
                inputTopic.PipeInput("key2", "b");
                inputTopic.PipeInput("key2", "c");

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                var resultK1 = store.Get("key1");
                var resultK2 = store.Get("key2");

                Assert.AreEqual("a", resultK1);
                Assert.AreEqual("c", resultK2);

                var results = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value));
                Assert.AreEqual(expected, results);
            }
        }

        [Test]
        public void KTableToStreamWithDelete()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", InMemory<string, string>.As("table-topic-store"))
                .ToStream().To("table-stream");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("table-stream");
                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create("key1", "a"));
                expected.Add(KeyValuePair.Create("key2", "b"));
                expected.Add(KeyValuePair.Create("key2", (string)null));

                inputTopic.PipeInput("key1", "a");
                inputTopic.PipeInput("key2", "b");
                inputTopic.PipeInput("key2", null);

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                var resultK1 = store.Get("key1");
                var resultK2 = store.Get("key2");

                Assert.AreEqual("a", resultK1);
                Assert.AreEqual(null, resultK2);

                var results = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value));
                Assert.AreEqual(expected, results);
            }
        }
    }
}
