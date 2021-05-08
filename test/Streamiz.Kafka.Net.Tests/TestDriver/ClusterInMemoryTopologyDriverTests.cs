using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using System;
using System.Linq;
using System.Threading;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    // TODO : Refactor for IWindowStore tests
    public class ClusterInMemoryTopologyDriverTests
    {
        [Test]
        public void StartDriverOK()
        {
            var source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";

            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";


            var builder = new StreamBuilder();
            builder.Stream<string, string>("test").To("test2");

            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), source.Token);
            driver.StartDriver();
            source.Cancel();
            driver.Dispose();
        }

        [Test]
        public void StartDriverKO()
        {
            var source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";

            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            Assert.Throws<StreamsException>(() =>
            {
                var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), source.Token);
                driver.StartDriver();
            });
            source.Cancel();
        }

        [Test]
        public void GetStateStoreNotExist()
        {
            var source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";

            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";


            var builder = new StreamBuilder();
            builder.Stream<string, string>("test").To("test2");

            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, source.Token);
            driver.StartDriver();
            driver.CreateInputTopic("test", new StringSerDes(), new StringSerDes());
            var store = driver.GetStateStore<string, string>("store");
            Assert.IsNull(store);
            source.Cancel();
            driver.Dispose();
        }

        [Test]
        public void GetStateStoreExist()
        {
            var source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";
            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            builder.Table<string, string>("test", InMemory<string, string>.As("store"));
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), source.Token);
            driver.StartDriver();
            var input = driver.CreateInputTopic("test", new StringSerDes(), new StringSerDes());
            var store = driver.GetStateStore<string, string>("store");

            Assert.IsNotNull(store);
            Assert.IsInstanceOf<MockReadOnlyKeyValueStore<string, string>>(store);
            input.PipeInput("coucou", "1");
            Thread.Sleep(100);

            Assert.AreEqual(1, ((MockReadOnlyKeyValueStore<string, string>)store).All().Count());
            Assert.AreEqual("1", ((MockReadOnlyKeyValueStore<string, string>)store).Get("coucou"));
            source.Cancel();
            driver.Dispose();
        }

        [Test]
        public void StateStoreUpdateKey()
        {
            var source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";
            config.PollMs = 50;
            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            builder.Table("test", InMemory<string, string>.As("store"));
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), source.Token);
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
            source.Cancel();
            driver.Dispose();
        }

        [Test]
        public void StateStoreDeleteKey()
        {
            var source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";
            config.PollMs = 100;
            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            builder.Table<string, string>("test", InMemory<string, string>.As("store"));
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), source.Token);
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
            Thread.Sleep(1000);

            Assert.AreEqual(0, ((MockReadOnlyKeyValueStore<string, string>)store).All().Count());
            Assert.AreEqual(null, ((MockReadOnlyKeyValueStore<string, string>)store).Get("coucou"));
            source.Cancel();
            driver.Dispose();
        }

        [Test]
        public void StateStoreReverseAll()
        {
            var source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";
            config.PollMs = 50;
            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            builder.Table("test", InMemory<string, string>.As("store"));
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), source.Token);
            driver.StartDriver();
            var input = driver.CreateInputTopic("test", new StringSerDes(), new StringSerDes());
            var store = driver.GetStateStore<string, string>("store");

            Assert.IsNotNull(store);
            Assert.IsInstanceOf<MockReadOnlyKeyValueStore<string, string>>(store);
            input.PipeInput("coucou", "1");
            input.PipeInput("test", "2");
            Thread.Sleep(100);

            Assert.AreEqual(2, ((MockReadOnlyKeyValueStore<string, string>)store).ReverseAll().Count());
            Assert.AreEqual("1", ((MockReadOnlyKeyValueStore<string, string>)store).Get("coucou"));
            Assert.AreEqual("2", ((MockReadOnlyKeyValueStore<string, string>)store).Get("test"));

            source.Cancel();
            driver.Dispose();
        }

        [Test]
        public void StateStoreRange()
        {
            var source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-config";
            config.PollMs = 50;
            var topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{config.ApplicationId}";

            var builder = new StreamBuilder();
            builder.Table("test", InMemory<string, string>.As("store"));
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), source.Token);
            driver.StartDriver();
            var input = driver.CreateInputTopic("test", new StringSerDes(), new StringSerDes());
            var store = driver.GetStateStore<string, string>("store");

            Assert.IsNotNull(store);
            Assert.IsInstanceOf<MockReadOnlyKeyValueStore<string, string>>(store);
            input.PipeInput("coucou", "1");
            input.PipeInput("test", "2");
            Thread.Sleep(100);

            var range = ((MockReadOnlyKeyValueStore<string, string>)store).Range("coucou", "test").ToList();
            Assert.AreEqual(2, range.Count);
            Assert.AreEqual("coucou", range[0].Key);
            Assert.AreEqual("1", range[0].Value);
            Assert.AreEqual("test", range[1].Key);
            Assert.AreEqual("2", range[1].Value);


            var reverseRange = ((MockReadOnlyKeyValueStore<string, string>)store).ReverseRange("coucou", "test").ToList();
            Assert.AreEqual(2, reverseRange.Count);
            Assert.AreEqual("test", reverseRange[0].Key);
            Assert.AreEqual("2", reverseRange[0].Value);
            Assert.AreEqual("coucou", reverseRange[1].Key);
            Assert.AreEqual("1", reverseRange[1].Value);


            source.Cancel();
            driver.Dispose();
        }
    }
}
