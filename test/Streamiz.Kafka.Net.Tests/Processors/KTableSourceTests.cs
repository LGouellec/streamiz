using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableSourceTests
    {
        [Test]
        public void ShouldNotAllowNullOrEmptyTopic()
        {
            var builder = new StreamBuilder();
            Assert.Throws<ArgumentException>(() => builder.Table<string, string>(null));
            Assert.Throws<ArgumentException>(() => builder.Table<string, string>(""));
        }

        [Test]
        public void SimpleKTableSource()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", InMemory<string, string>.As("table-topic-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInput("key1", "1");
                inputTopic.PipeInput("key2", "2");

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                var resultK1 = store.Get("key1");
                var resultK2 = store.Get("key2");

                Assert.AreEqual("1", resultK1);
                Assert.AreEqual("2", resultK2);
            }
        }

        [Test]
        public void KTableSourceUpdateKey()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", InMemory<string, string>.As("table-topic-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInput("key1", "1");
                inputTopic.PipeInput("key2", "2");

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                var resultK1 = store.Get("key1");
                var resultK2 = store.Get("key2");

                Assert.AreEqual("1", resultK1);
                Assert.AreEqual("2", resultK2);

                inputTopic.PipeInput("key1", "11");

                resultK1 = store.Get("key1");
                resultK2 = store.Get("key2");

                Assert.AreEqual("11", resultK1);
                Assert.AreEqual("2", resultK2);
            }
        }

        [Test]
        public void KTableSourceNoMaterialize()
        {
            var builder = new StreamBuilder();

            builder.Table<string, string>("table-topic");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInput("key1", "1");
                inputTopic.PipeInput("key2", "2");
            }
        }
    }
}
