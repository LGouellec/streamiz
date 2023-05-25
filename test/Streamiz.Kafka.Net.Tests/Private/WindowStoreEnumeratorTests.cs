using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory;
using System;
using System.Text;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.State.Metered;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class WindowStoreEnumeratorTests
    {
        [Test]
        public void WindowStoreEnumeratorWithSerdes()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("key"));
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(10),
                (long) TimeSpan.FromSeconds(1).TotalMilliseconds, false);
            store.Put(key, Encoding.UTF8.GetBytes("value"), date.GetMilliseconds());

            var enumerator = new MeteredWindowEnumerator<string>(
                store.Fetch(key, date.AddSeconds(-1), date.AddSeconds(1)),
                (b) => (new StringSerDes()).Deserialize(b, new SerializationContext()),
                new NoRunnableSensor("s", "s", MetricsRecordingLevel.INFO));
            var items = enumerator.ToList();
            Assert.AreEqual(1, items.Count);
            Assert.AreEqual("value", items[0].Value);
            Assert.AreEqual(date.GetMilliseconds(), items[0].Key);
        }

        [Test]
        public void WindowStoreEnumeratorTestNext()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("key"));
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(10),
                (long) TimeSpan.FromSeconds(1).TotalMilliseconds, false);
            store.Put(key, Encoding.UTF8.GetBytes("value"), date.GetMilliseconds());

            var enumerator = new MeteredWindowEnumerator<string>(
                store.Fetch(key, date.AddSeconds(-1), date.AddSeconds(1)),
                (b) => (new StringSerDes()).Deserialize(b, new SerializationContext()),
                new NoRunnableSensor("s", "s", MetricsRecordingLevel.INFO));
            int i = 0;
            while (enumerator.MoveNext())
            {
                Assert.AreEqual(date.GetMilliseconds(), enumerator.Current.Value.Key);
                Assert.AreEqual("value", enumerator.Current.Value.Value);
                ++i;
            }

            Assert.AreEqual(1, i);
        }

        [Test]
        public void WindowStoreEnumeratorTestReset()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("key"));
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(10),
                (long) TimeSpan.FromSeconds(1).TotalMilliseconds, false);
            store.Put(key, Encoding.UTF8.GetBytes("value"), date.GetMilliseconds());

            var enumerator = new MeteredWindowEnumerator<string>(
                store.Fetch(key, date.AddSeconds(-1), date.AddSeconds(1)),
                (b) => (new StringSerDes()).Deserialize(b, new SerializationContext()),
                new NoRunnableSensor("s", "s", MetricsRecordingLevel.INFO));
            int i = 0;
            while (enumerator.MoveNext())
            {
                Assert.AreEqual(date.GetMilliseconds(), enumerator.Current.Value.Key);
                Assert.AreEqual("value", enumerator.Current.Value.Value);
                ++i;
            }

            Assert.AreEqual(1, i);
            enumerator.Reset();
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual(date.GetMilliseconds(), enumerator.Current.Value.Key);
            Assert.AreEqual("value", enumerator.Current.Value.Value);
        }

        [Test]
        public void WindowStoreEnumeratorTestDispose()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("key"));
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(10),
                (long) TimeSpan.FromSeconds(1).TotalMilliseconds, false);
            store.Put(key, Encoding.UTF8.GetBytes("value"), date.GetMilliseconds());

            var enumerator = new MeteredWindowEnumerator<string>(
                store.Fetch(key, date.AddSeconds(-1), date.AddSeconds(1)),
                (b) => (new StringSerDes()).Deserialize(b, new SerializationContext()),
                new NoRunnableSensor("s", "s", MetricsRecordingLevel.INFO));
            enumerator.Dispose();
            Assert.Throws<ObjectDisposedException>(() => enumerator.MoveNext());
        }
    }
}