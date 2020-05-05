using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class InMemoryKeyValueStoreTests
    {
        [Test]
        public void CreateInMemoryKeyValueStore()
        {
            var store = new InMemoryKeyValueStore("store");
            Assert.IsFalse(store.Persistent);
            Assert.AreEqual("store", store.Name);
            Assert.AreEqual(0, store.ApproximateNumEntries());
        }

        [Test]
        public void PutKeyNotExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key"), value = serdes.Serialize("value");
            var store = new InMemoryKeyValueStore("store");
            store.Put(new Bytes(key), value);
            Assert.AreEqual(1, store.ApproximateNumEntries());
        }

        [Test]
        public void PutKeyExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key"),
                value = serdes.Serialize("value"),
                value2 = serdes.Serialize("value2");

            var store = new InMemoryKeyValueStore("store");
            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key), value2);
            Assert.AreEqual(1, store.ApproximateNumEntries());
            var v = store.Get(new Bytes(key));
            Assert.AreEqual("value2", serdes.Deserialize(v));
        }

        [Test]
        public void DeletKeyNotExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key");

            var store = new InMemoryKeyValueStore("store");
            var r = store.Delete(new Bytes(key));
            Assert.IsNull(r);
            Assert.AreEqual(0, store.ApproximateNumEntries());
        }

        [Test]
        public void DeleteKeyExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key"),
                value = serdes.Serialize("value");

            var store = new InMemoryKeyValueStore("store");
            store.Put(new Bytes(key), value);
            Assert.AreEqual(1, store.ApproximateNumEntries());
            var v = store.Delete(new Bytes(key));
            Assert.AreEqual(0, store.ApproximateNumEntries());
            Assert.AreEqual("value", serdes.Deserialize(v));
        }

        [Test]
        public void PutAll()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key"), value = serdes.Serialize("value");
            byte[] key1 = serdes.Serialize("key1"), value1 = serdes.Serialize("value1");
            byte[] key2 = serdes.Serialize("key2"), value2 = serdes.Serialize("value2");
            byte[] key3 = serdes.Serialize("key3"), value3 = serdes.Serialize("value3");

            var store = new InMemoryKeyValueStore("store");

            var items = new List<KeyValuePair<Bytes, byte[]>>();
            items.Add(KeyValuePair.Create(new Bytes(key), value));
            items.Add(KeyValuePair.Create(new Bytes(key1), value1));
            items.Add(KeyValuePair.Create(new Bytes(key2), value2));
            items.Add(KeyValuePair.Create(new Bytes(key3), value3));

            store.PutAll(items);

            Assert.AreEqual(4, store.ApproximateNumEntries());
        }

        [Test]
        public void PutIfAbsent()
        {
            var serdes = new StringSerDes();
            byte[] key3 = serdes.Serialize("key3"), value3 = serdes.Serialize("value3");

            var store = new InMemoryKeyValueStore("store");

            store.PutIfAbsent(new Bytes(key3), value3);
            store.PutIfAbsent(new Bytes(key3), value3);

            Assert.AreEqual(1, store.ApproximateNumEntries());
        }
    }
}
