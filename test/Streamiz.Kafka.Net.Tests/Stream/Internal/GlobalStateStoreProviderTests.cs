using System.Collections.Generic;
using System.Linq;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.Tests.Stream.Internal
{
    public class GlobalStateStoreProviderTests
    {
        IKeyValueStore<object, object> kvStore;
        TimestampedKeyValueStore<object, object> timestampedKVStore;
        IDictionary<string, IStateStore> stores;

        [SetUp]
        public void Setup()
        {
            kvStore = this.CreateMockStore<IKeyValueStore<object, object>>();
            timestampedKVStore = this.CreateMockStore<TimestampedKeyValueStore<object, object>>();
            stores = new Dictionary<string, IStateStore> { { "kv-store", kvStore }, { "ts-kv-store", timestampedKVStore } };
        }

        [Test]
        public void ShouldReturnEmptyItemListIfStoreDoesntExist()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(new Dictionary<string, IStateStore>());

            var result = provider.Stores(StoreQueryParameters.FromNameAndType("test", QueryableStoreTypes.KeyValueStore<object, object>()));

            Assert.AreEqual(0, result.Count());
        }

        [Test]
        public void ShouldThrowExceptionIfStoreIsNotOpen()
        {
            var mockStore = CreateMockStore<IKeyValueStore<object, object>>(isOpen: false);
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(new Dictionary<string, IStateStore> { { "test", mockStore } });

            Assert.Throws<InvalidStateStoreException>(() => provider.Stores(StoreQueryParameters.FromNameAndType("test", QueryableStoreTypes.KeyValueStore<object, object>())));
        }

        [Test]
        public void ShouldReturnKeyValueStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);

            var result = provider.Stores(StoreQueryParameters.FromNameAndType("kv-store", QueryableStoreTypes.KeyValueStore<object, object>()));

            Assert.AreEqual(kvStore, result.Single());
        }

        [Test]
        public void ShouldReturnTimestampedKeyValueStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);

            var result = provider.Stores(StoreQueryParameters.FromNameAndType("ts-kv-store", QueryableStoreTypes.TimestampedKeyValueStore<object, object>()));

            Assert.AreEqual(timestampedKVStore, result.Single());
        }

        [Test]
        public void ShouldNotReturnKeyValueStoreAsTimestampedStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);

            var result = provider.Stores(StoreQueryParameters.FromNameAndType("kv-store", QueryableStoreTypes.TimestampedKeyValueStore<object, object>()));

            Assert.AreEqual(0, result.Count());
        }

        [Test]
        public void ShouldReturnTimestampedKeyValueStoreAsKeyValueStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);

            var result = provider.Stores(StoreQueryParameters.FromNameAndType("ts-kv-store", QueryableStoreTypes.KeyValueStore<object, object>()));

            Assert.IsInstanceOf(typeof(ReadOnlyKeyValueStore<object, object>), result.Single());
            Assert.IsNotInstanceOf(typeof(TimestampedKeyValueStore<object, object>), result.Single());
        }

        private T CreateMockStore<T>(bool isOpen = true) where T : class, IStateStore
        {
            var kvStore = new Mock<T>();
            kvStore.Setup(kvStore => kvStore.IsOpen).Returns(isOpen);

            return kvStore.Object;
        }
    }
}
