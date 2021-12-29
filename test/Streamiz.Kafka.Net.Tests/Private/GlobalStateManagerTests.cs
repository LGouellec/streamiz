using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Mock.Sync;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class GlobalStateManagerTests
    {
        private GlobalStateManager stateManager;
        private Mock<IKeyValueStore<object, object>> kvStoreMock;
        private Mock<IKeyValueStore<object, object>> otherStoreMock;
        private Mock<IAdminClient> adminClientMock;
        private ProcessorTopology topology;
        private Mock<IStreamConfig> streamConfigMock;
        private ProcessorContext context;

        private readonly string kvStoreName = "kv-store";
        private readonly string kvStoreTopic = "kv-store-topic";
        private readonly string otherStoreName = "other-store";
        private readonly string otherStoreTopic = "other-store-topic";

        [SetUp]
        public void SetUp()
        {
            var mockSupplier = new SyncKafkaSupplier();
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "global-consulmer";
            var globalConsumer = mockSupplier.GetConsumer(consumerConfig, null);
            
            streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(c => c.StateDir).Returns(".");
            streamConfigMock.Setup(c => c.ApplicationId).Returns("app");

            kvStoreMock = CreateMockStore<IKeyValueStore<object, object>>(kvStoreName);
            otherStoreMock = CreateMockStore<IKeyValueStore<object, object>>(otherStoreName);
            var globalStateStores = new Dictionary<string, IStateStore>() {
                { kvStoreMock.Object.Name, kvStoreMock.Object },
                { otherStoreMock.Object.Name, otherStoreMock.Object }
            };
            var storesToTopics = new Dictionary<string, string>() {
                { kvStoreMock.Object.Name, kvStoreTopic },
                { otherStoreMock.Object.Name, otherStoreTopic }
            };

            topology = new ProcessorTopology(
                    null,
                    new Dictionary<string, IProcessor>(),
                    new Dictionary<string, IProcessor>(),
                    new Dictionary<string, IProcessor>(),
                    new Dictionary<string, IStateStore>(),
                    globalStateStores,
                    storesToTopics);

            adminClientMock = new Mock<IAdminClient>();
            RegisterPartitionInAdminClient(kvStoreTopic);
            RegisterPartitionInAdminClient(otherStoreTopic);

            stateManager = new GlobalStateManager(globalConsumer, topology,
                    adminClientMock.Object,
                    streamConfigMock.Object
                );

            context = new GlobalProcessorContext(
                streamConfigMock.Object,
                stateManager);
            
            stateManager.SetGlobalProcessorContext(context);
        }

        [Test]
        public void ShouldInitializeStateStores()
        {
            stateManager.Initialize();

            kvStoreMock.Verify(store => store.Init(It.IsAny<ProcessorContext>(), It.IsAny<IStateStore>()), Times.Once);
        }

        [Test]
        public void ShouldInitializeWithGlobalContext()
        {
            stateManager.Initialize();

            kvStoreMock.Verify(store => store.Init(context, It.IsAny<IStateStore>()), Times.Once);
        }

        [Test]
        public void ShouldReturnInitializedStoreNames()
        {
            ISet<string> storeNames = stateManager.Initialize();

            Assert.AreEqual(new HashSet<string>() { kvStoreName, otherStoreName }, storeNames);
        }

        [Test]
        public void ShouldThrowIfSameStoreTwiceTwiceInTopology()
        {
            // this was already registered once
            topology.GlobalStateStores.Add($"kvStoreMock.Object.Name{1}", kvStoreMock.Object);

            Assert.Throws<ArgumentException>(() => stateManager.Initialize());
        }

        [Test]
        public void ShouldThrowIfNoPartitionsFoundForStore()
        {
            kvStoreMock
                .Setup(s => s.Init(context, It.IsAny<IStateStore>()))
                .Callback((ProcessorContext c, IStateStore store) =>
                {
                    stateManager.Register(store, (k, v) => { });
                });
            
            adminClientMock.Setup(client => client.GetMetadata(kvStoreTopic, It.IsAny<TimeSpan>())).Returns((Metadata)null);
            Assert.Throws<StreamsException>(() => stateManager.Initialize());
        }

        [Test]
        public void ShouldSetChangelogOffsetToZero()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);

            Assert.AreEqual(-2, stateManager.ChangelogOffsets.Single(x => x.Key.Topic == kvStoreTopic).Value);
        }

        [Test]
        public void ShouldReturnStore()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);

            var result = stateManager.GetStore(kvStoreName);

            Assert.AreEqual(kvStoreMock.Object, result);
        }

        [Test]
        public void ShouldReturnNullIfNoStore()
        {
            stateManager.Initialize();

            var result = stateManager.GetStore("some-store-name");

            Assert.AreEqual(null, result);
        }

        [Test]
        public void ShouldFlushStateStores()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);
            stateManager.Register(otherStoreMock.Object, null);

            stateManager.Flush();

            kvStoreMock.Verify(x => x.Flush(), Times.Once);
            otherStoreMock.Verify(x => x.Flush(), Times.Once);
        }

        [Test]
        public void ShouldCloseStateStores()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);
            stateManager.Register(otherStoreMock.Object, null);

            stateManager.Close();

            kvStoreMock.Verify(x => x.Close(), Times.Once);
            otherStoreMock.Verify(x => x.Close(), Times.Once);
        }

        [Test]
        public void ShouldThrowIfStoreCloseFailed()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);
            kvStoreMock.Setup(x => x.Close()).Throws(new ProcessorStateException("boom!"));

            Assert.Throws<ProcessorStateException>(() => stateManager.Close());
        }

        [Test]
        public void ShouldAttemptToCloseAllStoresEvenWhenSomeException()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);
            stateManager.Register(otherStoreMock.Object, null);

            kvStoreMock.Setup(x => x.Close()).Throws(new ProcessorStateException("boom!"));

            try
            {
                stateManager.Close();
            }
            catch
            {
                // expected
            }

            kvStoreMock.Verify(x => x.Close(), Times.Once);
            otherStoreMock.Verify(x => x.Close(), Times.Once);
        }

        private Mock<T> CreateMockStore<T>(string name, bool isOpen = true) where T : class, IStateStore
        {
            var store = new Mock<T>();
            store.Setup(kvStore => kvStore.Name).Returns(name);
            store.Setup(kvStore => kvStore.IsOpen).Returns(isOpen);
            return store;
        }

        private void RegisterPartitionInAdminClient(string topic)
        {
            adminClientMock.Setup(client => client.GetMetadata(topic, It.IsAny<TimeSpan>())).Returns(
                new Metadata(
                    null,
                    new List<TopicMetadata>() {
                        new TopicMetadata(
                            topic,
                            new List<PartitionMetadata>() { new PartitionMetadata(0, 0, new int[] { }, new int[] { }, null) },
                            null)
                    }, 0, "")
            );
        }
    }
}
