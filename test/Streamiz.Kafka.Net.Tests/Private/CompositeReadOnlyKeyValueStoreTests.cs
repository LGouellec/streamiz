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
using Streamiz.Kafka.Net.State.Metered;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class CompositeReadOnlyKeyValueStoreTests
    {
        class MockStateProvider<K, V> : IStateStoreProvider<IReadOnlyKeyValueStore<K, V>, K, V>
        {
            private readonly List<ITimestampedKeyValueStore<K, V>> stores = new List<ITimestampedKeyValueStore<K, V>>();

            public MockStateProvider(long windowSize, ISerDes<K> keySerdes, ISerDes<V> valueSerdes,
                params InMemoryKeyValueStore[] stores)
            {
                this.stores = stores
                    .Select(s => new MeteredTimestampedKeyValueStore<K, V>(s, keySerdes,
                        new ValueAndTimestampSerDes<V>(valueSerdes), "test-scope"))
                    .Cast<ITimestampedKeyValueStore<K, V>>()
                    .ToList();
            }

            public IEnumerable<IReadOnlyKeyValueStore<K, V>> Stores(string storeName,
                IQueryableStoreType<IReadOnlyKeyValueStore<K, V>, K, V> queryableStoreType)
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
            var bytes = Bytes.Wrap("test"u8.ToArray());
            var provider =
                new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1,
                    store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()));
            var result = composite.Get("test");
            Assert.IsNotNull(result);
            Assert.AreEqual("coucou1", result);
        }
        
        [Test]
        public void GetKeyNotPresentTest()
        {
            InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
            InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");
            var provider =
                new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1,
                    store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            var result = composite.Get("test");
            Assert.IsNull(result);
        }


        [Test]
        public void AllTest()
        {
            InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
            InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = Bytes.Wrap("test"u8.ToArray());
            var bytes2 = Bytes.Wrap("test2"u8.ToArray());
            var provider =
                new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1,
                    store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()));
            store2.Put(bytes2,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt), new SerializationContext()));
            var result = composite.All().ToList();
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("coucou1", result.FirstOrDefault(k => k.Key.Equals("test")).Value);
            Assert.AreEqual("coucou2", result.FirstOrDefault(k => k.Key.Equals("test2")).Value);
        }

        [Test]
        public void ReverseAllTest()
        {
            InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
            InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = Bytes.Wrap("test"u8.ToArray());
            var bytes2 = Bytes.Wrap("test2"u8.ToArray());
            var provider =
                new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1,
                    store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()));
            store1.Put(bytes2,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1bis", dt), new SerializationContext()));
            store2.Put(bytes,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt), new SerializationContext()));
            store2.Put(bytes2,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2bis", dt), new SerializationContext()));
            var result = composite.ReverseAll().ToList();
            Assert.IsNotNull(result);
            Assert.AreEqual(4, result.Count);
            Assert.AreEqual("test2", result[0].Key);
            Assert.AreEqual("coucou1bis", result[0].Value);
            Assert.AreEqual("test", result[1].Key);
            Assert.AreEqual("coucou1", result[1].Value);
            Assert.AreEqual("test2", result[2].Key);
            Assert.AreEqual("coucou2bis", result[2].Value);
            Assert.AreEqual("test", result[3].Key);
            Assert.AreEqual("coucou2", result[3].Value);
        }

        [Test]
        public void CountTest()
        {
            InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
            InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = Bytes.Wrap("test"u8.ToArray());
            var bytes2 = Bytes.Wrap("test2"u8.ToArray());
            var provider =
                new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1,
                    store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()));
            store2.Put(bytes2,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt), new SerializationContext()));
            Assert.AreEqual(2, composite.ApproximateNumEntries());
        }

        [Test]
        public void ResetTest()
        {
            InMemoryKeyValueStore store1 = new InMemoryKeyValueStore("store");
            InMemoryKeyValueStore store2 = new InMemoryKeyValueStore("store");
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = Bytes.Wrap("test"u8.ToArray());
            var bytes2 = Bytes.Wrap("test2"u8.ToArray());
            var provider =
                new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1,
                    store2);
            var composite = new CompositeReadOnlyKeyValueStore<string, string>(provider, storeType, "store");
            store1.Put(bytes,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()));
            store2.Put(bytes2,
                valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt), new SerializationContext()));
            var enumerator = composite.Range("test", "test2");
            Assert.IsNotNull(enumerator);
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("test", enumerator.PeekNextKey());
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("test2", enumerator.PeekNextKey());
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Reset();
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("test", enumerator.PeekNextKey());
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("test2", enumerator.PeekNextKey());
            Assert.IsFalse(enumerator.MoveNext());
        }
    }
}