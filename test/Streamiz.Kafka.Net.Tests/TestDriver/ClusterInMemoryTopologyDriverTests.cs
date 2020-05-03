using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    public class ClusterInMemoryTopologyDriverTests
    {
        [Test]
        public void StartDriverOK()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";

            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";


            var builder = new StreamBuilder();
            builder.Stream<string, string>("test").To("test2");

            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, default);
            driver.StartDriver();
        }

        [Test]
        public void StartDriverKO()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";

            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), default);
            Assert.Throws<StreamsException>(() => driver.StartDriver());
        }

        [Test]
        public void GetStateStoreNotExist()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";

            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";


            var builder = new StreamBuilder();
            builder.Stream<string, string>("test").To("test2");

            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, default);
            driver.StartDriver(); 
            driver.CreateInputTopic("test", new StringSerDes(), new StringSerDes());
            var store = driver.GetStateStore<string, string>("store");
            Assert.IsNull(store);
        }

        [Test]
        public void GetStateStoreExist()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";
            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            builder.Table<string, string>("test", InMemory<string, string>.As("store"));
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, default);
            driver.StartDriver();
            var input = driver.CreateInputTopic("test", new StringSerDes(), new StringSerDes());
            var store = driver.GetStateStore<string, string>("store");

            Assert.IsNotNull(store);
            Assert.IsInstanceOf<ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>>(store);
            input.PipeInput("coucou", "1");
            Thread.Sleep(100);

            Assert.AreEqual(1, ((ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>)store).All().Count());
            Assert.AreEqual("1", ((ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>)store).Get("coucou").Value);
        }

        [Test]
        public void StateStoreUpdateKey()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";
            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            builder.Table<string, string>("test", InMemory<string, string>.As("store"));
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, default);
            driver.StartDriver();
            var input = driver.CreateInputTopic("test", new StringSerDes(), new StringSerDes());
            var store = driver.GetStateStore<string, string>("store");

            Assert.IsNotNull(store);
            Assert.IsInstanceOf<MockReadOnlyKeyValueStore<string, string>>(store);
            input.PipeInput("coucou", "1");
            Thread.Sleep(100);

            Assert.AreEqual(1, ((MockReadOnlyKeyValueStore<string, string>)store).All().Count());
            Assert.AreEqual("1", ((MockReadOnlyKeyValueStore<string, string>)store).Get("coucou"));

            input.PipeInput("coucou", "2");
            Thread.Sleep(100);

            Assert.AreEqual(1, ((MockReadOnlyKeyValueStore<string, string>)store).All().Count());
            Assert.AreEqual("2", ((MockReadOnlyKeyValueStore<string, string>)store).Get("coucou"));
        }

        [Test]
        public void StateStoreDeleteKey()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";
            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            builder.Table<string, string>("test", InMemory<string, string>.As("store"));
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, default);
            driver.StartDriver();
            var input = driver.CreateInputTopic("test", new StringSerDes(), new StringSerDes());
            var store = driver.GetStateStore<string, string>("store");

            Assert.IsNotNull(store);
            Assert.IsInstanceOf<MockReadOnlyKeyValueStore<string, string>>(store);
            input.PipeInput("coucou", "1");
            Thread.Sleep(100);

            Assert.AreEqual(1, ((MockReadOnlyKeyValueStore<string, string>)store).All().Count());
            Assert.AreEqual("1", ((MockReadOnlyKeyValueStore<string, string>)store).Get("coucou"));

            input.PipeInput("coucou", null);
            Thread.Sleep(100);

            Assert.AreEqual(0, ((MockReadOnlyKeyValueStore<string, string>)store).All().Count());
            Assert.AreEqual(null, ((MockReadOnlyKeyValueStore<string, string>)store).Get("coucou"));
        }

    }
}
