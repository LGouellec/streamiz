using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableSourceTests
    {
        private class MyITimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                return DateTime.Now.GetMilliseconds();
            }
        }


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
        public void SimpleKTableSource2()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", 
                new StringSerDes(), new StringSerDes(),
                InMemory<string, string>.As("table-topic-store"));

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
        public void SimpleKTableSource3()
        {
            var builder = new StreamBuilder();

            builder.Table<string, string, StringSerDes, StringSerDes>
                ("table-topic", InMemory<string, string>.As("table-topic-store"));

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
        public void SimpleKTableSource4()
        {
            var builder = new StreamBuilder();

            builder.Table<string, string, StringSerDes, StringSerDes>
                ("table-topic", InMemory<string, string>.As("table-topic-store"), "table");

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
        public void SimpleKTableSource5()
        {
            var builder = new StreamBuilder();

            builder.Table<string, string, StringSerDes, StringSerDes>
                ("table-topic", InMemory<string, string>.As("table-topic-store"), new MyITimestampExtractor());

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
        public void SimpleKTableSource6()
        {
            var builder = new StreamBuilder();

            builder.Table<string, string, StringSerDes, StringSerDes>
                ("table-topic", InMemory<string, string>.As("table-topic-store"), "table", new MyITimestampExtractor());

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
        public void KTableSourceKeyNull()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", InMemory<string, string>.As("table-topic-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInput(null, "1");

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(0, store.ApproximateNumEntries());
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
        public void KTableSourceDelete()
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

                Assert.AreEqual(2, store.All().Count());
                Assert.AreEqual("1", resultK1);
                Assert.AreEqual("2", resultK2);

                inputTopic.PipeInput("key1", null);

                resultK1 = store.Get("key1");
                resultK2 = store.Get("key2");

                Assert.AreEqual(1, store.All().Count());
                Assert.AreEqual(null, resultK1);
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

        [Test]
        public void KTableSourceNoMaterialize2()
        {
            var builder = new StreamBuilder();

            builder.Table<string, string, StringSerDes, StringSerDes>("table-topic");

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
