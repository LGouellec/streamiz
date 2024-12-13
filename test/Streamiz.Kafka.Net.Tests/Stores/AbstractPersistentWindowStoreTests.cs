using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Moq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Stores
{

    public abstract class AbstractPersistentWindowStoreTests
    {
        protected static int MAX_CACHE_SIZE_BYTES = 150;
        protected static long DEFAULT_TIMESTAMP = 10L;
        protected static long WINDOW_SIZE = 10L;
        protected static long SEGMENT_INTERVAL = 100L;
        protected static TimeSpan RETENTION_MS = TimeSpan.FromMilliseconds(100);

        private StreamConfig config;
        private IWindowStore<Bytes, byte[]> store;
        private ProcessorContext context;
        private TaskId id;
        private TopicPartition partition;
        private ProcessorStateManager stateManager;
        private Mock<AbstractTask> task;
        internal WindowKeySchema keySchema;
        private CachingWindowStore cachingStore;

        protected abstract IWindowStore<Bytes, byte[]> GetBackWindowStore();

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = "unit-test-rocksdb-w";
            config.DefaultStateStoreCacheMaxBytes = MAX_CACHE_SIZE_BYTES;

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

            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());

            keySchema = new WindowKeySchema();
            store = GetBackWindowStore();
            cachingStore = new CachingWindowStore(store,
                WINDOW_SIZE, SEGMENT_INTERVAL, keySchema, null);

            if (store.Persistent)
                config.UseRandomRocksDbConfigForTest();

            cachingStore.Init(context, cachingStore);
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "source"));
        }

        [TearDown]
        public void End()
        {
            store.Flush();
            stateManager.Close();
            if (store.Persistent)
                config.RemoveRocksDbFolderForTest();
        }

        #region Private

        private static Bytes BytesKey(String key)
        {
            return Bytes.Wrap(Encoding.UTF8.GetBytes(key));
        }

        private String StringFrom(byte[] from)
        {
            return Encoding.UTF8.GetString(from);
        }

        private static byte[] BytesValue(String value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        private static void VerifyWindowedKeyValue(
            KeyValuePair<Windowed<Bytes>, byte[]>? actual,
            Windowed<Bytes> expectedKey,
            String expectedValue)
        {
            Assert.NotNull(actual);
            Assert.AreEqual(expectedKey.Window, actual.Value.Key.Window);
            Assert.AreEqual(expectedKey.Key.Get, actual.Value.Key.Key.Get);
            Assert.AreEqual(expectedKey.Window, actual.Value.Key.Window);
            Assert.AreEqual(expectedValue, Encoding.UTF8.GetString(actual.Value.Value));
        }

        #endregion

        #region Tests

        [Test]
        public void ShouldPutFetchFromCache()
        {
            cachingStore.Put(BytesKey("a"), BytesValue("a"), DEFAULT_TIMESTAMP);
            cachingStore.Put(BytesKey("b"), BytesValue("b"), DEFAULT_TIMESTAMP);

            Assert.AreEqual(BytesValue("a"), cachingStore.Fetch(BytesKey("a"), DEFAULT_TIMESTAMP));
            Assert.AreEqual(BytesValue("b"), cachingStore.Fetch(BytesKey("b"), DEFAULT_TIMESTAMP));
            Assert.Null(cachingStore.Fetch(BytesKey("c"), 10L));
            Assert.Null(cachingStore.Fetch(BytesKey("a"), 0L));

            using var enumeratorA = cachingStore.Fetch(BytesKey("a"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
            using var enumeratorB = cachingStore.Fetch(BytesKey("b"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
            Assert.IsTrue(enumeratorA.MoveNext());
            Assert.IsTrue(enumeratorB.MoveNext());
            Assert.AreEqual(BytesValue("a"), enumeratorA.Current.Value.Value);
            Assert.AreEqual(DEFAULT_TIMESTAMP, enumeratorA.Current.Value.Key);
            Assert.AreEqual(BytesValue("b"), enumeratorB.Current.Value.Value);
            Assert.AreEqual(DEFAULT_TIMESTAMP, enumeratorB.Current.Value.Key);
            Assert.IsFalse(enumeratorA.MoveNext());
            Assert.IsFalse(enumeratorB.MoveNext());
            Assert.AreEqual(2, cachingStore.Cache.Count);
        }

        [Test]
        public void ShouldPutFetchWithRealTimestampFromCache()
        {
            DateTime now = DateTime.Now;
            cachingStore.Put(BytesKey("a"), BytesValue("a"), now.GetMilliseconds());
            cachingStore.Put(BytesKey("b"), BytesValue("b"), now.GetMilliseconds() + 4);

            Assert.AreEqual(BytesValue("a"), cachingStore.Fetch(BytesKey("a"), now.GetMilliseconds()));
            Assert.AreEqual(BytesValue("b"), cachingStore.Fetch(BytesKey("b"), now.GetMilliseconds() + 4));
            Assert.Null(cachingStore.Fetch(BytesKey("c"), now.GetMilliseconds() + 10));
            Assert.Null(cachingStore.Fetch(BytesKey("a"), now.GetMilliseconds() + 10));

            using var enumeratorA = cachingStore.Fetch(BytesKey("a"), now.GetMilliseconds(), now.GetMilliseconds());
            using var enumeratorB =
                cachingStore.Fetch(BytesKey("b"), now.GetMilliseconds() + 4, now.GetMilliseconds() + 4);
            Assert.IsTrue(enumeratorA.MoveNext());
            Assert.IsTrue(enumeratorB.MoveNext());
            Assert.AreEqual(BytesValue("a"), enumeratorA.Current.Value.Value);
            Assert.AreEqual(now.GetMilliseconds(), enumeratorA.Current.Value.Key);
            Assert.AreEqual(BytesValue("b"), enumeratorB.Current.Value.Value);
            Assert.AreEqual(now.GetMilliseconds() + 4, enumeratorB.Current.Value.Key);
            Assert.IsFalse(enumeratorA.MoveNext());
            Assert.IsFalse(enumeratorB.MoveNext());
            Assert.AreEqual(2, cachingStore.Cache.Count);
        }

        [Test]
        public void ShouldFetchAllWithinTimestampRange()
        {
            DateTime now = DateTime.Now;
            String[] array = { "a", "b", "c", "d", "e", "f", "g", "h" };
            for (int i = 0; i < array.Length; i++)
            {
                cachingStore.Put(BytesKey(array[i]), BytesValue(array[i]), now.AddMilliseconds(i).GetMilliseconds());
            }

            using var enumerator = cachingStore.FetchAll(now, now.AddMilliseconds(7));
            for (int i = 0; i < array.Length; i++)
            {
                enumerator.MoveNext();
                String str = array[i];
                VerifyWindowedKeyValue(
                    enumerator.Current,
                    new Windowed<Bytes>(BytesKey(str),
                        new TimeWindow(now.AddMilliseconds(i).GetMilliseconds(),
                            now.AddMilliseconds(i).GetMilliseconds() + WINDOW_SIZE)),
                    str);
            }

            Assert.IsFalse(enumerator.MoveNext());

            using var enumerator1 = cachingStore.FetchAll(now.AddMilliseconds(2), now.AddMilliseconds(4));
            for (int i = 2; i <= 4; i++)
            {
                enumerator1.MoveNext();
                String str = array[i];
                VerifyWindowedKeyValue(
                    enumerator1.Current,
                    new Windowed<Bytes>(BytesKey(str),
                        new TimeWindow(now.AddMilliseconds(i).GetMilliseconds(),
                            now.AddMilliseconds(i).GetMilliseconds() + WINDOW_SIZE)),
                    str);
            }

            Assert.IsFalse(enumerator1.MoveNext());

            using var enumerator2 =
                cachingStore.FetchAll(now.AddMilliseconds(5), now.AddMilliseconds(7));
            for (int i = 5; i <= 7; i++)
            {
                enumerator2.MoveNext();
                String str = array[i];
                VerifyWindowedKeyValue(
                    enumerator2.Current,
                    new Windowed<Bytes>(BytesKey(str),
                        new TimeWindow(now.AddMilliseconds(i).GetMilliseconds(),
                            now.AddMilliseconds(i).GetMilliseconds() + WINDOW_SIZE)),
                    str);
            }

            Assert.IsFalse(enumerator2.MoveNext());
        }

        [Test]
        public void ShouldTakeValueFromCacheIfSameTimestampFlushedToRocks()
        {
            cachingStore.Put(BytesKey("1"), BytesValue("a"), DEFAULT_TIMESTAMP);
            cachingStore.Flush();
            cachingStore.Put(BytesKey("1"), BytesValue("b"), DEFAULT_TIMESTAMP);

            using var fetch = cachingStore.Fetch(BytesKey("1"), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
            Assert.IsTrue(fetch.MoveNext());
            Assert.NotNull(fetch.Current);
            Assert.AreEqual("b", Encoding.UTF8.GetString(fetch.Current.Value.Value));
            Assert.AreEqual(DEFAULT_TIMESTAMP, fetch.Current.Value.Key);
            Assert.IsFalse(fetch.MoveNext());
        }

        [Test]
        public void ShouldFetchAndIterateOverExactKeys()
        {
            cachingStore.Put(BytesKey("a"), BytesValue("0001"), 0);
            cachingStore.Put(BytesKey("aa"), BytesValue("0002"), 0);
            cachingStore.Put(BytesKey("a"), BytesValue("0003"), 1);
            cachingStore.Put(BytesKey("aa"), BytesValue("0004"), 1);
            cachingStore.Put(BytesKey("a"), BytesValue("0005"), SEGMENT_INTERVAL);

            List<KeyValuePair<long, byte[]>> expected = new List<KeyValuePair<long, byte[]>>
            {
                new(0L, BytesValue("0001")),
                new(1L, BytesValue("0003")),
                new(SEGMENT_INTERVAL, BytesValue("0005"))
            };

            using var enumerator = cachingStore.Fetch(BytesKey("a"), 0L, long.MaxValue);
            var actual = enumerator.ToList();
            AssertExtensions.VerifyKeyValueList(expected, actual);
        }

        [Test]
        public void ShouldGetAllFromCache()
        {
            cachingStore.Put(BytesKey("a"), BytesValue("a"), DEFAULT_TIMESTAMP);
            cachingStore.Put(BytesKey("b"), BytesValue("b"), DEFAULT_TIMESTAMP);
            cachingStore.Put(BytesKey("c"), BytesValue("c"), DEFAULT_TIMESTAMP);
            cachingStore.Put(BytesKey("d"), BytesValue("d"), DEFAULT_TIMESTAMP);
            cachingStore.Put(BytesKey("e"), BytesValue("e"), DEFAULT_TIMESTAMP);
            cachingStore.Put(BytesKey("f"), BytesValue("f"), DEFAULT_TIMESTAMP);
            cachingStore.Put(BytesKey("g"), BytesValue("g"), DEFAULT_TIMESTAMP);
            cachingStore.Put(BytesKey("h"), BytesValue("h"), DEFAULT_TIMESTAMP);

            using var enumerator = cachingStore.All();
            String[] array = { "a", "b", "c", "d", "e", "f", "g", "h" };
            foreach (var s in array)
            {
                enumerator.MoveNext();
                VerifyWindowedKeyValue(
                    enumerator.Current,
                    new Windowed<Bytes>(BytesKey(s),
                        new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                    s);
            }

            Assert.IsFalse(enumerator.MoveNext());
        }

        #endregion
    }
}