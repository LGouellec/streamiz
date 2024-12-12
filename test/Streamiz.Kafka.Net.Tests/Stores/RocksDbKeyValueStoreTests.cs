﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Avro.Util;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class RocksDbKeyValueStoreTests
    {
        private StreamConfig config;
        private RocksDbKeyValueStore store;
        private ProcessorContext context;
        private TaskId id;
        private TopicPartition partition;
        private ProcessorStateManager stateManager;
        private Mock<AbstractTask> task;
        private StreamMetricsRegistry metricsRegistry;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = "unit-test-rocksdb-kv";
            config.UseRandomRocksDbConfigForTest();

            id = new TaskId { Id = 0, Partition = 0 };
            partition = new TopicPartition("source", 0);
            stateManager = new ProcessorStateManager(
                id,
                new List<TopicPartition> { partition },
                null,
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);

            metricsRegistry = new StreamMetricsRegistry();
            context = new ProcessorContext(task.Object, config, stateManager, metricsRegistry);

            store = new RocksDbKeyValueStore("test-store", "rocksdb");
            store.Init(context, store);
        }

        [TearDown]
        public void End()
        {
            if (store != null)
            {
                store.Flush();
                stateManager.Close();
            }
            config.RemoveRocksDbFolderForTest();
        }

        [Test]
        public void TestConfig()
        {
            Assert.AreEqual($"{Path.Combine(config.StateDir, config.ApplicationId, id.ToString())}", context.StateDir);
        }

        [Test]
        public void CreateRocksDbKeyValueStore()
        {
            Assert.IsTrue(store.Persistent);
            Assert.AreEqual("test-store", store.Name);
        }

        [Test]
        public void PutKeyNotExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            store.Put(new Bytes(key), value);
            Assert.AreEqual(1, store.ApproximateNumEntries());
        }

        [Test]
        public void PutKeyExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()),
                value = serdes.Serialize("value", new SerializationContext()),
                value2 = serdes.Serialize("value2", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key), value2);
            var e = store.All().ToList();
            Assert.AreEqual(1, e.Count);
            var v = store.Get(new Bytes(key));
            Assert.AreEqual("value2", serdes.Deserialize(v, new SerializationContext()));
        }

        [Test]
        public void DeletKeyNotExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext());

            var r = store.Delete(new Bytes(key));
            Assert.IsNull(r);
            Assert.AreEqual(0, store.ApproximateNumEntries());
        }

        [Test]
        public void DeleteKeyExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()),
                value = serdes.Serialize("value", new SerializationContext());

            store.Put(new Bytes(key), value);
            Assert.AreEqual(1, store.ApproximateNumEntries());
            var v = store.Delete(new Bytes(key));
            Assert.AreEqual(0, store.ApproximateNumEntries());
            Assert.AreEqual("value", serdes.Deserialize(v, new SerializationContext()));
        }

        [Test]
        public void PutAll()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key1 = serdes.Serialize("key1", new SerializationContext()), value1 = serdes.Serialize("value1", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            var items = new List<KeyValuePair<Bytes, byte[]>>();
            items.Add(KeyValuePair.Create(new Bytes(key), value));
            items.Add(KeyValuePair.Create(new Bytes(key1), value1));
            items.Add(KeyValuePair.Create(new Bytes(key2), value2));
            items.Add(KeyValuePair.Create(new Bytes(key3), value3));

            store.PutAll(items);

            Assert.AreEqual(4, store.ApproximateNumEntries());
        }

        [Test]
        public void PutAllWithValueNull()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());

            var items = new List<KeyValuePair<Bytes, byte[]>>();
            items.Add(KeyValuePair.Create(new Bytes(key), value));
            items.Add(KeyValuePair.Create(new Bytes(key), (byte[])null));

            store.PutAll(items);

            Assert.AreEqual(0, store.ApproximateNumEntries());
        }

        [Test]
        public void PutIfAbsent()
        {
            var serdes = new StringSerDes();
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            store.PutIfAbsent(new Bytes(key3), value3);
            store.PutIfAbsent(new Bytes(key3), value3);

            Assert.AreEqual(1, store.ApproximateNumEntries());
        }

        [Test]
        public void EmptyEnumerator()
        {
            var enumerator = store.All().GetEnumerator();
            Assert.Throws<NotMoreValueException>(() =>
            {
                var a = enumerator.Current;
            });
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorReset()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());

            store.Put(new Bytes(key), value);

            var enumerator = store.All().GetEnumerator();
            Assert.IsTrue(enumerator.MoveNext());
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Reset();
            Assert.IsTrue(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorReverseAll()
        {
            var serdes = new StringSerDes();

            string deserialize(byte[] bytes)
            {
                return serdes.Deserialize(bytes, new SerializationContext());
            }

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key2), value2);

            var enumerator = store.ReverseAll().GetEnumerator();
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key2", deserialize(enumerator.Current.Key.Get));
            Assert.AreEqual("value2", deserialize(enumerator.Current.Value));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key", deserialize(enumerator.Current.Key.Get));
            Assert.AreEqual("value", deserialize(enumerator.Current.Value));
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorRangeAll()
        {
            var serdes = new StringSerDes();

            string deserialize(byte[] bytes)
            {
                return serdes.Deserialize(bytes, new SerializationContext());
            }

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key2), value2);
            store.Put(new Bytes(key3), value3);

            var enumerator = store.Range(new Bytes(key), new Bytes(key2));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key", deserialize(enumerator.Current.Value.Key.Get));
            Assert.AreEqual("value", deserialize(enumerator.Current.Value.Value));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key2", deserialize(enumerator.Current.Value.Key.Get));
            Assert.AreEqual("value2", deserialize(enumerator.Current.Value.Value));
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorReverseRangeAll()
        {
            var serdes = new StringSerDes();

            string deserialize(byte[] bytes)
            {
                return serdes.Deserialize(bytes, new SerializationContext());
            }

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key2), value2);
            store.Put(new Bytes(key3), value3);

            var enumerator = store.ReverseRange(new Bytes(key), new Bytes(key2));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key2", deserialize(enumerator.Current.Value.Key.Get));
            Assert.AreEqual("value2", deserialize(enumerator.Current.Value.Value));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key", deserialize(enumerator.Current.Value.Key.Get));
            Assert.AreEqual("value", deserialize(enumerator.Current.Value.Value));
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorIncorrectRange()
        {
            var serdes = new StringSerDes();

            string deserialize(byte[] bytes)
            {
                return serdes.Deserialize(bytes, new SerializationContext());
            }

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key2), value2);
            store.Put(new Bytes(key3), value3);

            var enumerator = store.Range(new Bytes(key2), new Bytes(key));
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorAlreadyDispose()
        {
            var serdes = new StringSerDes();

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());

            store.Put(new Bytes(key), value);

            var enumerator = store.Range(new Bytes(key), new Bytes(key));
            Assert.IsTrue(enumerator.MoveNext());
            enumerator.Dispose();
            Assert.Throws<ObjectDisposedException>(() => enumerator.Dispose());
        }
        
        [Test]
        public void OpenEnumerator()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());

            store.Put(new Bytes(key), value);

            var enumerator = store.Range(new Bytes(key), new Bytes(key));
            Assert.IsTrue(enumerator.MoveNext());
            store.Close();
            Assert.Throws<ObjectDisposedException>(() => enumerator.Dispose());
            store.OpenDatabase(context); // needed for teardown
        }
    }
}
