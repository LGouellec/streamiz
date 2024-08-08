using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableFilterTests
    {
        [Test]
        public void ShouldNotAllowNullFilterAction()
        {
            var builder = new StreamBuilder();
            var table = builder.Table<string, string>("ktable-topic");
            Assert.Throws<ArgumentNullException>(() => table.Filter(null));
        }

        [Test]
        public void FilterWithElements()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Table<string, string>("table-topic")
                .Filter((k, v, _) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase), InMemory.As<string, string>("test-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInputs(data);

                var store = driver.GetKeyValueStore<string, string>("test-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(store.All().Count(), 2);

                var r1 = store.Get("key1");
                var r2 = store.Get("key2");
                Assert.AreEqual("test1234", r1);
                Assert.AreEqual("test", r2);
            }
        }

        [Test]
        public void FilterNoElements()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key2", "car"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Table<string, string>("table-topic")
                .Filter((k, v, _) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase), InMemory.As<string, string>("test-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInputs(data);

                var store = driver.GetKeyValueStore<string, string>("test-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(store.All().Count(), 0);

                var r = store.Get("key2");
                Assert.AreEqual(null, r);
            }
        }

        [Test]
        public void FilterWithOneOutputElement()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "ferrari"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Table<string, string>("table-topic")
                .Filter((k, v, _) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase), InMemory.As<string, string>("test-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInputs(data);

                var store = driver.GetKeyValueStore<string, string>("test-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(store.All().Count(), 1);

                var r1 = store.Get("key1");
                var r2 = store.Get("key2");
                Assert.AreEqual("test1234", r1);
                Assert.AreEqual(null, r2);
            }
        }
    }
}
