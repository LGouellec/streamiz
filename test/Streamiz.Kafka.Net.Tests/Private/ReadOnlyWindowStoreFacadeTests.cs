using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal;
using System;
using Streamiz.Kafka.Net.State.Metered;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class ReadOnlyWindowStoreFacadeTests
    {
        private ReadOnlyWindowStoreFacade<string, int> facade = null;
        private ITimestampedWindowStore<string, int> store = null;
        private InMemoryWindowStore inmemorystore = null;

        [SetUp]
        public void Setup()
        {
            inmemorystore = new InMemoryWindowStore("store", TimeSpan.FromMinutes(20), 1000 * 2, false);
            store = new MeteredTimestampedWindowStore<string, int>(inmemorystore, 1000 * 2, new StringSerDes(),
                new ValueAndTimestampSerDes<int>(new Int32SerDes()), "in-memory-window");
            facade = new ReadOnlyWindowStoreFacade<string, int>(store);
        }

        [Test]
        public void TestFacadeFetchKeyByTime()
        {
            DateTime dt = DateTime.Now;
            store.Put(
                "coucou",
                ValueAndTimestamp<int>.Make(120, dt.AddMilliseconds(100).GetMilliseconds()),
                dt.GetMilliseconds());
            var r = facade.Fetch("coucou", dt.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(120, r);
        }

        [Test]
        public void TestFacadeFetchKeyOneElement()
        {
            DateTime dt = DateTime.Now;
            store.Put(
                "coucou",
                ValueAndTimestamp<int>.Make(120, dt.AddMilliseconds(100).GetMilliseconds()),
                dt.GetMilliseconds());
            store.Put(
                "coucou",
                ValueAndTimestamp<int>.Make(500, dt.AddMilliseconds(400).GetMilliseconds()),
                dt.GetMilliseconds());
            var r = facade.Fetch("coucou", dt.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(500, r);
        }

        [Test]
        public void TestFacadeFetchRangeKeyOneElement()
        {
            DateTime dt = DateTime.Now;
            store.Put(
                "coucou",
                ValueAndTimestamp<int>.Make(120, dt.AddMilliseconds(100).GetMilliseconds()),
                dt.GetMilliseconds());
            store.Put(
                "coucou",
                ValueAndTimestamp<int>.Make(500, dt.AddMilliseconds(400).GetMilliseconds()),
                dt.GetMilliseconds());
            var enumerator = facade.Fetch("coucou", dt.AddSeconds(-2), dt.AddSeconds(5));
            Assert.IsNotNull(enumerator);
            var list = enumerator.ToList();
            Assert.AreEqual(1, list.Count);
            Assert.AreEqual(500, list[0].Value);
            Assert.AreEqual(dt.GetMilliseconds(), list[0].Key);
        }

        [Test]
        public void TestFacadeFetchRangeKeyTwoElement()
        {
            DateTime dt = DateTime.Now;
            store.Put(
                "coucou",
                ValueAndTimestamp<int>.Make(120, dt.AddMilliseconds(100).GetMilliseconds()),
                dt.GetMilliseconds());
            store.Put(
                "coucou-bis",
                ValueAndTimestamp<int>.Make(500, dt.AddMilliseconds(400).GetMilliseconds()),
                dt.AddMilliseconds(100).GetMilliseconds());
            var enumerator = facade.FetchAll(dt.AddSeconds(-2), dt.AddSeconds(5));
            Assert.IsNotNull(enumerator);
            var list = enumerator.ToList();
            Assert.AreEqual(2, list.Count);
            Assert.AreEqual(120, list[0].Value);
            Assert.AreEqual("coucou", list[0].Key.Key);
            Assert.AreEqual(TimeSpan.FromSeconds(2), list[0].Key.Window.TotalTime);
            Assert.AreEqual(dt.GetMilliseconds(), list[0].Key.Window.StartMs);
            Assert.AreEqual(500, list[1].Value);
            Assert.AreEqual("coucou-bis", list[1].Key.Key);
            Assert.AreEqual(TimeSpan.FromSeconds(2), list[1].Key.Window.TotalTime);
            Assert.AreEqual(dt.AddMilliseconds(100).GetMilliseconds(), list[1].Key.Window.StartMs);
        }
    }
}