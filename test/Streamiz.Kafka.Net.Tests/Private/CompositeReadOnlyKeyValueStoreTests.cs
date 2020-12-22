using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Private
{

    public class CompositeReadOnlyKeyValueStoreTests
    {
        class MockStateProvider<K, V> : IStateStoreProvider<IReadOnlyKeyValueStore<K, V>, K, V>
        {
            private readonly List<ITimestampedKeyValueStore<K, V>> stores = new List<ITimestampedKeyValueStore<K, V>>();

            public MockStateProvider(long windowSize, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, params InMemoryKeyValueStore[] stores)
            {
                this.stores = stores
                                .Select(s => new TimestampedKeyValueStoreImpl<K, V>(s, keySerdes, new ValueAndTimestampSerDes<V>(valueSerdes)))
                                .Cast<ITimestampedKeyValueStore<K, V>>()
                                .ToList();
            }

            public IEnumerable<IReadOnlyKeyValueStore<K, V>> Stores(string storeName, IQueryableStoreType<IReadOnlyKeyValueStore<K, V>, K, V> queryableStoreType)
            {
                return
                    stores
                        .Where(s => s.Name.Equals(storeName))
                        .Select<ITimestampedKeyValueStore<K, V>, IReadOnlyKeyValueStore<K, V>>(s =>
                        {
                            if (s is IReadOnlyKeyValueStore<K, V>)
                            {
                                return s as IReadOnlyKeyValueStore<K, V>;
                            }
                            else if (s is ITimestampedKeyValueStore<K, V>)
                            {
                                return new ReadOnlyKeyValueStoreFacade<K, V>(s);
                            }
                            else
                            {
                                return null;
                            }
                        });
            }

        }
        private readonly KeyValueStoreType<string, string> storeType = new KeyValueStoreType<string, string>();

        [Test]
        public void GetTest()
        {
            InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
            InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()));
            var result = composite.Get("test");
            Assert.IsNotNull(result);
            Assert.AreEqual("coucou1", result);
        }

        [Test]
        public void AllTest()
        {
            InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
            InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var bytes2 = new Bytes(Encoding.UTF8.GetBytes("test2"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()));
            store2.Put(bytes2, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt), new SerializationContext()));
            var result = composite.All().ToList();
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("coucou1", result.FirstOrDefault(k => k.Key.Equals("test")).Value);
            Assert.AreEqual("coucou2", result.FirstOrDefault(k => k.Key.Equals("test2")).Value);
        }

        [Test]
        public void CountTest()
        {
            InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
            InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var bytes2 = new Bytes(Encoding.UTF8.GetBytes("test2"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()));
            store2.Put(bytes2, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt), new SerializationContext()));
            Assert.AreEqual(2, composite.ApproximateNumEntries());
        }
    }
}
