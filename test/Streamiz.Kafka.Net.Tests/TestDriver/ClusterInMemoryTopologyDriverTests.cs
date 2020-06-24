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
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    // TODO : Refactor for WindowStore tests
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
            var driver = new ClusterInMemoryTopologyDriver("client", builder.Build().Builder, config, topicConfiguration, TimeSpan.FromSeconds(1), source.Token);
            Assert.Throws<StreamsException>(() => driver.StartDriver());
            source.Cancel();
            driver.Dispose();
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
            config.PollMs = 1;
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
            config.PollMs = 1;
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
            Thread.Sleep(100);

            Assert.AreEqual(0, ((MockReadOnlyKeyValueStore<string, string>)store).All().Count());
            Assert.AreEqual(null, ((MockReadOnlyKeyValueStore<string, string>)store).Get("coucou"));
            source.Cancel();
            driver.Dispose();
        }
      
    }
}
