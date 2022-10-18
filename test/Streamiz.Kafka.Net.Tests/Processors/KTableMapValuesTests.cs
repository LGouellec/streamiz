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
    public class KTableMapValuesTests
    {
        [Test]
        public void ShouldNotAllowNullMapValuesAction()
        {
            var builder = new StreamBuilder();
            var table = builder.Table<string, string>("ktable-topic");
            Func<string, string> mapper1 = null;
            Func<string, string, string> mapper3 = null;
            IValueMapper<string, string> mapper2 = null;
            IValueMapperWithKey<string, string, string> mapper4 = null;
            Assert.Throws<ArgumentNullException>(() => table.MapValues(mapper1));
            Assert.Throws<ArgumentNullException>(() => table.MapValues(mapper2));
            Assert.Throws<ArgumentNullException>(() => table.MapValues(mapper3));
            Assert.Throws<ArgumentNullException>(() => table.MapValues(mapper4));
        }

        [Test]
        public void MapValuesOtherValueType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Table<string, string>("table-topic")
                .MapValues((v) => v.Length, InMemory.As<string,int>("test-store").With<StringSerDes, Int32SerDes>());
                
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-mapvalues";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInputs(data);

                var store = driver.GetKeyValueStore<string, int>("test-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(store.All().Count(), 3);

                var r1 = store.Get("key1");
                var r2 = store.Get("key2");
                var r3 = store.Get("key3");
                Assert.AreEqual(8, r1);
                Assert.AreEqual(4, r2);
                Assert.AreEqual(5, r3);
            }
        }

        [Test]
        public void MapValuesSameValueType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Table<string, string>("table-topic")
                .MapValues((v) => v.ToCharArray()[0].ToString(), InMemory.As<string,string>("test-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-mapvalues";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInputs(data);

                var store = driver.GetKeyValueStore<string, string>("test-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(store.All().Count(), 3);

                var r1 = store.Get("key1");
                var r2 = store.Get("key2");
                var r3 = store.Get("key3");
                Assert.AreEqual("t", r1);
                Assert.AreEqual("t", r2);
                Assert.AreEqual("p", r3);
            }
        }

        [Test]
        public void MapValuesNoStateStore()
        {
            var builder = new StreamBuilder();
            var observed = new List<KeyValuePair<string, int>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Table<string, string>("table-topic")
                .MapValues((v) => v.Length)
                .ToStream()
                .Peek((k, v) => observed.Add(KeyValuePair.Create(k, v)));

            var expected = new List<KeyValuePair<string, int>>();
            expected.Add(KeyValuePair.Create("key1", 8));
            expected.Add(KeyValuePair.Create("key2", 4));
            expected.Add(KeyValuePair.Create("key3", 5));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-mapvalues";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInputs(data);

                var store = driver.GetKeyValueStore<string, int>("test-store");
                Assert.IsNull(store);
                Assert.AreEqual(expected, observed);
            }
        }
    }
}
