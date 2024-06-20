using System;
using System.Text;
using System.Threading;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class InMemoryWindowStoreTests
    {
        private static readonly TimeSpan defaultRetention = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan defaultSize = TimeSpan.FromSeconds(10);
        
        [Test]
        public void CreateInMemoryWindowStore()
        {
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            Assert.IsFalse(store.Persistent);
            Assert.AreEqual("store", store.Name);
            Assert.AreEqual(0, store.All().ToList().Count);
        }

        [Test]
        public void PutOneElement()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);
        }

        [Test]
        public void PutTwoElementSameKeyDifferentTime()
        {
            var date = DateTime.Now;
            var dt2 = date.AddSeconds(1);
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key, BitConverter.GetBytes(150), dt2.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);
            
            r = store.Fetch(key, dt2.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(150), r);
        }

        [Test]
        public void PutTwoElementSameKeySameTime()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key, BitConverter.GetBytes(300), date.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(300), r);
        }

        [Test]
        public void PutTwoElementDifferentKeyDifferentTime()
        {
            var date = DateTime.Now;
            var dt2 = date.AddSeconds(1);
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var key2 = new Bytes(Encoding.UTF8.GetBytes("coucou-key"));
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key2, BitConverter.GetBytes(300), dt2.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);

            r = store.Fetch(key, dt2.GetMilliseconds());
            Assert.IsNull(r);

            r = store.Fetch(key2, dt2.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(300), r);

            r = store.Fetch(key2, date.GetMilliseconds());
            Assert.IsNull(r);
        }

        [Test]
        public void PutTwoElementDifferentKeySameTime()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var key2 = new Bytes(Encoding.UTF8.GetBytes("coucou-key"));
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key2, BitConverter.GetBytes(300), date.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);
            r = store.Fetch(key2, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(300), r);
        }
    
        [Test]
        public void PutElementsAndFetch()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var key2 = new Bytes(Encoding.UTF8.GetBytes("coucou-key"));
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key2, BitConverter.GetBytes(300), date.AddSeconds(1).GetMilliseconds());
            var r = store.FetchAll(date.AddSeconds(-10), date.AddSeconds(20)).ToList();
            Assert.AreEqual(2, r.Count);
            Assert.AreEqual(key, r[0].Key.Key);
            Assert.AreEqual(BitConverter.GetBytes(100), r[0].Value);
            Assert.AreEqual(defaultSize, r[0].Key.Window.TotalTime);
            Assert.AreEqual(key2, r[1].Key.Key);
            Assert.AreEqual(BitConverter.GetBytes(300), r[1].Value);
            Assert.AreEqual(defaultSize, r[1].Key.Window.TotalTime);
        }

        [Test]
        public void PutElementsWithNullValue()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, null, date.GetMilliseconds());
            var r = store.All().ToList();
            Assert.AreEqual(0, r.Count);
        }

        [Test]
        public void PutElementsAndUpdateNullValueSameWindow()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var value = Encoding.UTF8.GetBytes("test");
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, value, date.GetMilliseconds());
            store.Put(key, null, date.GetMilliseconds());
            var r = store.All().ToList();
            Assert.AreEqual(0, r.Count);
        }

        [Test]
        public void PutElementsAndUpdateNullValueDifferentWindow()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var value = Encoding.UTF8.GetBytes("test");
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, value, date.GetMilliseconds());
            store.Put(key, null, date.AddSeconds(1).GetMilliseconds());
            var r = store.All().ToList();
            Assert.AreEqual(1, r.Count);
            Assert.AreEqual(value, store.Fetch(key, date.GetMilliseconds()));
            Assert.IsNull(store.Fetch(key, date.AddSeconds(1).GetMilliseconds()));
        }


        [Test]
        public void FetchKeyDoesNotExist()
        {
            var date = DateTime.Now;
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            Assert.IsNull(store.Fetch(new Bytes(new byte[0]), 100));
        }

        [Test]
        public void FetchRangeDoesNotExist()
        {
            var date = DateTime.Now;
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, false);
            var it = store.FetchAll(date.AddDays(-1), date.AddDays(1));
            Assert.AreEqual(null, it.Current);
            Assert.IsFalse(it.MoveNext());
            Assert.AreEqual(null, it.Current);
        }

        [Test]
        public void TestRetention()
        {
            var metricsRegistry = new StreamMetricsRegistry();
            var mockContext = new Mock<ProcessorContext>();
            mockContext.Setup(c => c.Id).Returns(new TaskId{Id = 0, Partition = 0});
            mockContext.Setup(c => c.Metrics).Returns(metricsRegistry);
            mockContext.Setup(c => c.Timestamp).Returns(DateTime.Now.GetMilliseconds());
            
            var date = DateTime.Now.AddDays(-1);
            var store = new InMemoryWindowStore("store", TimeSpan.Zero, (long)defaultSize.TotalMilliseconds, false);
            store.Init(mockContext.Object, null);
            store.Put(new Bytes(new byte[1] { 13}), new byte[0], date.GetMilliseconds());
            Assert.AreEqual(0, store.All().ToList().Count);
        }

        [Test]
        public void TestRetentionWithOpenIt()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var value = Encoding.UTF8.GetBytes("test");
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(1), (long)defaultSize.TotalMilliseconds, false);
            store.Put(key, value, date.GetMilliseconds());
            var it = store.All();
            it.MoveNext();
            Thread.Sleep(2000);
            store.Put(key, value, date.AddSeconds(4).GetMilliseconds());
            var r = it.ToList().Count;
            Assert.AreEqual(0, r);
        }

        [Test]
        public void EmptyKeyValueIteratorTest()
        {
            var dt = DateTime.Now;
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(1), (long)defaultSize.TotalMilliseconds, false);
            var enumerator = store.FetchAll(dt.AddDays(1), dt);
            Assert.IsAssignableFrom<EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>>(enumerator);
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Reset();
            Assert.AreEqual(0, enumerator.ToList().Count);
        }

        [Test]
        public void EmptyWindowStoreIteratorTest()
        {
            var dt = DateTime.Now;
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(1), (long)defaultSize.TotalMilliseconds, false);
            var enumerator = store.Fetch(new Bytes(null), dt.AddDays(1), dt);
            Assert.IsAssignableFrom<EmptyWindowStoreEnumerator<byte[]>>(enumerator);
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Reset();
            Assert.AreEqual(0, enumerator.ToList().Count);
        }
        
        [Test]
        public void DuplicateEvents()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test"));
            var store = new InMemoryWindowStore("store", defaultRetention, (long)defaultSize.TotalMilliseconds, true);
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key, BitConverter.GetBytes(200), date.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds(), date.GetMilliseconds());
            var items = r.ToList();
            Assert.AreEqual(2, items.Count);
            Assert.AreEqual(100, Math.Abs(BitConverter.ToInt32(items[0].Value, 0) - BitConverter.ToInt32(items[1].Value, 0)));
        }
        
        [Test]
        public void FetchDuplicateEvents()
        {
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(1), (long)defaultSize.TotalMilliseconds, true);
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var key2 = new Bytes(Encoding.UTF8.GetBytes("test-key2"));
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key, BitConverter.GetBytes(150), date.GetMilliseconds());
            store.Put(key2, BitConverter.GetBytes(300), date.GetMilliseconds());
            
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            // InMemoryStore doesn't keep the order insertion for the same key
            // Assert.AreEqual(BitConverter.GetBytes(100), r);
        }
    }
}
