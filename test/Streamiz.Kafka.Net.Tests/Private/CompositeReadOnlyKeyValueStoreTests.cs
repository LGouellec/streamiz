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
        class MockStateProvider<K, V> : IStateStoreProvider<ReadOnlyKeyValueStore<K, V>, K, V>
        {
            private readonly List<TimestampedKeyValueStore<K, V>> stores = new List<TimestampedKeyValueStore<K, V>>();

            public MockStateProvider(long windowSize, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, params InMemoryKeyValueStore[] stores)
            {
                this.stores = stores
                                .Select(s => new TimestampedKeyValueStoreImpl<K, V>(s, keySerdes, new ValueAndTimestampSerDes<V>(valueSerdes)))
                                .Cast<TimestampedKeyValueStore<K, V>>()
                                .ToList();
            }

            public IEnumerable<ReadOnlyKeyValueStore<K, V>> Stores(string storeName, IQueryableStoreType<ReadOnlyKeyValueStore<K, V>, K, V> queryableStoreType)
            {
                return
                    stores
                        .Where(s => s.Name.Equals(storeName))
                        .Select<TimestampedKeyValueStore<K, V>, ReadOnlyKeyValueStore<K, V>>(s =>
                        {
                            if (s is ReadOnlyKeyValueStore<K, V>)
                            {
                                return s as ReadOnlyKeyValueStore<K, V>;
                            }
                            else if (s is TimestampedKeyValueStore<K, V>)
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
        private readonly InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
        private readonly InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");


        [Test]
        public void GetTest()
        {
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt)));
            var result = composite.Get("test");
            Assert.IsNotNull(result);
            Assert.AreEqual("coucou1", result);
        }

        [Test]
        public void AllTest()
        {
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var bytes2 = new Bytes(Encoding.UTF8.GetBytes("test2"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt)));
            store2.Put(bytes2, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt)));
            var result = composite.All().ToList();
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("coucou1", result.FirstOrDefault(k => k.Key.Equals("test")).Value);
            Assert.AreEqual("coucou2", result.FirstOrDefault(k => k.Key.Equals("test2")).Value);
        }

        [Test]
        public void CountTest()
        {
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var bytes2 = new Bytes(Encoding.UTF8.GetBytes("test2"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt)));
            store2.Put(bytes2, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt)));
            Assert.AreEqual(2, composite.ApproximateNumEntries());
        }
    }
}
