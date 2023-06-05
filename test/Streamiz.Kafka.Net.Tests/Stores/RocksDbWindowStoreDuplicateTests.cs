using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.State.RocksDb.Internal;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Streamiz.Kafka.Net.Metrics;


namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class RocksDbWindowStoreDuplicateTests
    {
        private static readonly TimeSpan defaultRetention = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan defaultSize = TimeSpan.FromSeconds(10);

        private StreamConfig config = null;
        private RocksDbWindowStore store = null;
        private ProcessorContext context = null;
        private TaskId id = null;
        private TopicPartition partition = null;
        private ProcessorStateManager stateManager = null;
        private Mock<AbstractTask> task = null;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = $"unit-test-duplicate-rocksdb-window";
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
                new RocksDbSegmentedBytesStore("test-w-store", (long)defaultRetention.TotalMilliseconds, 5000, new RocksDbWindowKeySchema()),
                (long)defaultSize.TotalMilliseconds, true);

            store.Init(context, store);
        }

        [TearDown]
        public void End()
        {
            store.Flush();
            stateManager.Close();
            config.RemoveRocksDbFolderForTest();
        }

        [Test]
        public void FetchDuplicateEvents()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var key2 = new Bytes(Encoding.UTF8.GetBytes("test-key2"));
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key, BitConverter.GetBytes(150), date.GetMilliseconds());
            store.Put(key2, BitConverter.GetBytes(300), date.GetMilliseconds());
            
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);
        }

    }
}