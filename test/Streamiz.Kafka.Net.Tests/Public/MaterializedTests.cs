using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class MaterializedTests
    {
        [Test]
        public void TestCreateKeyValueBytesStoreSupplier()
        {
            var m = Materialized<string, string, IKeyValueStore<Bytes, byte[]>>
                .Create(new InMemoryKeyValueBytesStoreSupplier("name"));
            Assert.IsNotNull(m);
            Assert.IsNotNull(m.StoreSupplier);
        }

        [Test]
        public void TestCreateKeyValueBytesStoreSupplierWithSerdes()
        {
            var m = Materialized<string, string, IKeyValueStore<Bytes, byte[]>>
                .Create<StringSerDes, StringSerDes>(new InMemoryKeyValueBytesStoreSupplier("name"));
            Assert.IsNotNull(m);
            Assert.IsNotNull(m.StoreSupplier);
            Assert.IsNotNull(m.KeySerdes);
            Assert.IsNotNull(m.ValueSerdes);
            Assert.IsAssignableFrom<StringSerDes>(m.KeySerdes);
            Assert.IsAssignableFrom<StringSerDes>(m.ValueSerdes);
        }

        [Test]
        public void TestCreateWithSerdes()
        {
            var m = Materialized<string, string, IKeyValueStore<Bytes, byte[]>>
                .Create<StringSerDes, StringSerDes>();
            Assert.IsNotNull(m);
            Assert.IsNotNull(m.KeySerdes);
            Assert.IsNotNull(m.ValueSerdes);
            Assert.IsAssignableFrom<StringSerDes>(m.KeySerdes);
            Assert.IsAssignableFrom<StringSerDes>(m.ValueSerdes);
        }

        [Test]
        public void TestWithKeySerdes()
        {
            var m = Materialized<string, string, IStateStore>.Create("test").WithKeySerdes(new StringSerDes());
            Assert.IsNotNull(m.KeySerdes);
            Assert.IsAssignableFrom<StringSerDes>(m.KeySerdes);
        }

        [Test]
        public void TestWithKeySerdes2()
        {
            var m = Materialized<string, string, IStateStore>.Create("test").WithKeySerdes<StringSerDes>();
            Assert.IsNotNull(m.KeySerdes);
            Assert.IsAssignableFrom<StringSerDes>(m.KeySerdes);
        }

        [Test]
        public void TestWithValueSerdes()
        {
            var m = Materialized<string, string, IStateStore>.Create("test").WithValueSerdes(new StringSerDes());
            Assert.IsNotNull(m.ValueSerdes);
            Assert.IsAssignableFrom<StringSerDes>(m.ValueSerdes);
        }

        [Test]
        public void TestWithValueSerdes2()
        {
            var m = Materialized<string, string, IStateStore>.Create("test").WithValueSerdes<StringSerDes>();
            Assert.IsNotNull(m.ValueSerdes);
            Assert.IsAssignableFrom<StringSerDes>(m.ValueSerdes);
        }
    
        [Test]
        public void TestWithLoggingEnabled()
        {
            var topicConfig = new Dictionary<string, string>
            {
                {"test", "test" }
            };
            var m = Materialized<string, string, IStateStore>
                .Create("test")
                .WithLoggingEnabled(topicConfig);
            Assert.IsNotNull(m);
            Assert.IsTrue(m.LoggingEnabled);
            Assert.AreEqual(topicConfig, m.TopicConfig);
        }

        [Test]
        public void TestWithLoggingDisabled()
        {
            var m = Materialized<string, string, IStateStore>
                .Create("test")
                .WithLoggingDisabled();
            Assert.IsNotNull(m);
            Assert.IsFalse(m.LoggingEnabled);
        }

        [Test]
        public void TestWithCachingEnabled()
        {
            var m = Materialized<string, string, IStateStore>
                .Create("test")
                .WithCachingEnabled();
            Assert.IsNotNull(m);
            Assert.IsTrue(m.CachingEnabled);
        }

        [Test]
        public void TestWithCachingDisabled()
        {
            var m = Materialized<string, string, IStateStore>
                .Create("test")
                .WithCachingDisabled();
            Assert.IsNotNull(m);
            Assert.IsFalse(m.CachingEnabled);
        }

        [Test]
        public void TestWithRetention()
        {
            var m = Materialized<string, string, IStateStore>
                .Create("test")
                .WithRetention(TimeSpan.FromSeconds(10));
            Assert.IsNotNull(m);
            Assert.AreEqual(TimeSpan.FromSeconds(10), m.Retention);
        }

        [Test]
        public void TestStoreName1()
        {
            var m = Materialized<string, string, IStateStore>
                .Create("test");

            Assert.IsNotNull(m);
            Assert.AreEqual("test", m.StoreName);
            Assert.IsNull(m.QueryableStoreName);
        }

        [Test]
        public void TestStoreName2()
        {
            var m = Materialized<string, string, IStateStore>
                .Create("test")
                .UseProvider(null, "");

            Assert.IsNotNull(m);
            Assert.AreEqual("test", m.StoreName);
            Assert.AreEqual("test", m.QueryableStoreName);
        }

        [Test]
        public void TestStoreName3()
        {
            var m = Materialized<string, string, IStateStore>
                .Create()
                .UseProvider(null, "");

            Assert.IsNotNull(m);
            Assert.AreEqual(string.Empty, m.StoreName);
            Assert.IsNull(m.QueryableStoreName);
        }

        private class MyNameProvider : INameProvider
        {
            public string NewProcessorName(string prefix)
                => $"processor-{prefix}";

            public string NewStoreName(string prefix)
                => $"store-{prefix}";
        }

        [Test]
        public void TestStoreName4()
        {
              var m = Materialized<string, string, IStateStore>
                .Create()
                .UseProvider(new MyNameProvider(), "pref");

            Assert.IsNotNull(m);
            Assert.AreEqual("store-pref", m.StoreName);
            Assert.AreEqual("store-pref", m.QueryableStoreName);
        }

        [Test]
        public void TestStoreName5()
        {
            var m = Materialized<string, string, IStateStore>
              .Create("store")
              .UseProvider(new MyNameProvider(), "pref");

            Assert.IsNotNull(m);
            Assert.AreEqual("store", m.StoreName);
            Assert.AreEqual("store", m.QueryableStoreName);
        }
    }
}
