using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class RocksDbWindowStoreIssue185Tests
    {
        private static readonly TimeSpan defaultRetention = TimeSpan.FromDays(1);
        private static readonly TimeSpan defaultSize = TimeSpan.FromHours(6);

        private StreamConfig config;
        private RocksDbWindowStore store;
        private ProcessorContext context;
        private TaskId id;
        private TopicPartition partition;
        private ProcessorStateManager stateManager;
        private Mock<AbstractTask> task;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = "unit-test-rocksdb-w";
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

            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());

            store = new RocksDbWindowStore(
                new RocksDbSegmentedBytesStore(
                    "test-w-store", 
                    (long)defaultRetention.TotalMilliseconds,
                    7200000,
                    new WindowKeySchema()),
                (long)defaultSize.TotalMilliseconds, false);

            store.Init(context, store);
        }

        [TearDown]
        public void End()
        {
            store.Flush();
            stateManager.Close();
            config.RemoveRocksDbFolderForTest();
        }

        private Bytes Key(string key)
        {
            return new Bytes(Encoding.UTF8.GetBytes(key));
        }

        private byte[] Value(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }
        
        [Test]
        public void ReproducerIssue185()
        {
            var date = DateTime.Now;
            var dt2 = DateTime.Now;
            dt2 = date.AddSeconds(30);
            for (int i = 0; i < 120; ++i)
            {
                store.Put(Key("key"), Value("value"+i), dt2.GetMilliseconds());
                dt2 = date.AddMinutes(i);
            }
            
            var dtTs = date.AddHours(2);
            long after = 10800000, before = 10800000; // window size 3 hours
      
            var r = store.Fetch(
                Key("key"),
                dtTs.GetMilliseconds() - before,
                dtTs.GetMilliseconds() + after);

            var elements = r.ToList();
            Console.WriteLine("Elements Fetch count " + elements.Count);
            Assert.AreEqual(120, elements.Count);
            var allElements = store.All().ToList();
            Console.WriteLine("All Elements Fetch count " + allElements.Count);
            Assert.AreEqual(120, allElements.Count);
        }
        
        [Test]
        public void FetchWindowMultipleKeys()
        {
            var date = DateTime.Now;
            var dt2 = DateTime.Now;
            dt2 = date.AddSeconds(30);
            for (int i = 0; i < 120; ++i)
            {
                store.Put(Key("key"), Value("value"+i), dt2.GetMilliseconds());
                store.Put(Key("key1"), Value("value"+i), dt2.GetMilliseconds());
                dt2 = date.AddMinutes(i);
            }
            
            var dtTs = date.AddHours(2);
            long after = 10800000, before = 10800000; // window size 3 hours
      
            var r = store.Fetch(
                Key("key"),
                dtTs.GetMilliseconds() - before,
                dtTs.GetMilliseconds() + after);

            var elements = r.ToList();
            Console.WriteLine("Elements Fetch count " + elements.Count);
            Assert.AreEqual(120, elements.Count);
            var allElements = store.All().ToList();
            Console.WriteLine("All Elements Fetch count " + allElements.Count);
            Assert.AreEqual(240, allElements.Count);
        }
        
        [Test]
        public void FetchWindowDifferentKey()
        {
            var date = DateTime.Now;
            var dt2 = DateTime.Now;
            dt2 = date.AddSeconds(30);
            for (int i = 0; i < 120; ++i)
            {
                store.Put(Key("key"), Value("value"+i), dt2.GetMilliseconds());
                store.Put(Key("key1"), Value("value"+i), dt2.GetMilliseconds());
                dt2 = date.AddMinutes(i);
            }
            
            var dtTs = date.AddHours(2);
            long after = 10800000, before = 10800000; // window size 3 hours
            
            var r = store.FetchAll(
                (dtTs.GetMilliseconds() - before).FromMilliseconds(),
                (dtTs.GetMilliseconds() + after).FromMilliseconds());

            var elements = r.ToList();
            Console.WriteLine("Elements Fetch count " + elements.Count);
            Assert.AreEqual(240, elements.Count);
            var allElements = store.All().ToList();
            Console.WriteLine("All Elements Fetch count " + allElements.Count);
            Assert.AreEqual(240, allElements.Count);
        }
        
        [Test]
        public void GetWindowStoreWithTimestamp()
        {
            var date = DateTime.Now;
            var dt2 = DateTime.Now;
            dt2 = date.AddSeconds(30);
            for (int i = 0; i < 120; ++i)
            {
                store.Put(Key("key"), Value("value"+i), dt2.GetMilliseconds());
                dt2 = date.AddMinutes(i);
            }

            var r = store.Fetch(
                Key("key"),
                date.AddSeconds(30).GetMilliseconds());
            
            Assert.IsNotNull(r);
            Assert.AreEqual("value0", Encoding.UTF8.GetString(r));
        }
    }
}