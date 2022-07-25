using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Tests.Helpers;
using ThreadState = Streamiz.Kafka.Net.Processors.ThreadState;

namespace Streamiz.Kafka.Net.Tests.Private
{
    // TODO : make abstract class with StreamThreadRestoreRocksDbTests
    public class StreamThreadRestoreInMemoryTests
    {
        private CancellationTokenSource token1;
        private CancellationTokenSource token2;

        private readonly StreamConfig<StringSerDes, StringSerDes> config =
            new StreamConfig<StringSerDes, StringSerDes>();

        private MockKafkaSupplier mockKafkaSupplier;
        private StreamThread thread1;
        private StreamThread thread2;
        private bool thread2Disposed = false;

        [SetUp]
        public void Init()
        {
            token1 = new System.Threading.CancellationTokenSource();
            token2 = new System.Threading.CancellationTokenSource();

            config.ApplicationId = "test-stream-thread";
            config.StateDir = Path.Combine(".", Guid.NewGuid().ToString());
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;

            mockKafkaSupplier = new MockKafkaSupplier(2, 0);

            var builder = new StreamBuilder();
            builder.Table("topic", InMemory<string, string>.As("store").WithLoggingEnabled());

            var topo = builder.Build();
            topo.Builder.RewriteTopology(config);
            topo.Builder.BuildTopology();

            thread1 = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, new StreamMetricsRegistry(), config,
                mockKafkaSupplier, mockKafkaSupplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread2 = StreamThread.Create(
                "thread-1", "c1",
                topo.Builder, new StreamMetricsRegistry(), config,
                mockKafkaSupplier, mockKafkaSupplier.GetAdmin(config.ToAdminConfig("admin")),
                1) as StreamThread;

            var internalTopicManager =
                new DefaultTopicManager(config, mockKafkaSupplier.GetAdmin(config.ToAdminConfig("admin")));
            InternalTopicManagerUtils
                .New()
                .CreateInternalTopicsAsync(internalTopicManager, topo.Builder).GetAwaiter();
        }

        [TearDown]
        public void Dispose()
        {
            if (!thread2Disposed)
            {
                token2.Cancel();
                thread2.Dispose();
            }

            token1.Cancel();
            thread1.Dispose();
            mockKafkaSupplier.Destroy();
        }


        #region StreamThread restore statefull topology

        [Test]
        public void StreamThreadRestorationPhaseStartDifferent()
        {
            var producerConfig = config.Clone();
            producerConfig.ApplicationId = "produce-test";

            var serdes = new StringSerDes();
            var producer = mockKafkaSupplier.GetProducer(producerConfig.ToProducerConfig());

            thread1.Start(token1.Token);
            Thread.Sleep(1500);
            thread2.Start(token2.Token);

            producer.Produce(new TopicPartition("topic", 0), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });
            producer.Produce(new TopicPartition("topic", 1), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key2", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            AssertExtensions.WaitUntil(
                () => thread1.State == ThreadState.RUNNING &&
                      thread2.State == ThreadState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            // 2 CONSUMER FOR THE SAME GROUP ID => TOPIC WITH 2 PARTITIONS
            Assert.AreEqual(1, thread1.ActiveTasks.Count());
            Assert.AreEqual(1, thread2.ActiveTasks.Count());

            AssertExtensions.WaitUntil(
                () => thread1.ActiveTasks.ToList()[0].State == TaskState.RUNNING &&
                      thread2.ActiveTasks.ToList()[0].State == TaskState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var storeThread1 =
                thread1.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;
            var storeThread2 =
                thread2.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;

            Assert.IsNotNull(storeThread1);
            Assert.IsNotNull(storeThread2);

            AssertExtensions.WaitUntil(
                () => storeThread1.All().ToList().Count == 1,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            AssertExtensions.WaitUntil(
                () => storeThread2.All().ToList().Count == 1,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var totalItemsSt1 = storeThread1.All().ToList();
            var totalItemsSt2 = storeThread2.All().ToList();

            Assert.AreEqual(1, totalItemsSt1.Count);
            Assert.AreEqual(1, totalItemsSt2.Count);

            // Thread2 closed, partitions assigned from thread2 rebalance to thread1
            // Thread1 need to restore state store
            token2.Cancel();
            thread2.Dispose();

            thread2Disposed = true;

            producer.Produce(new TopicPartition("topic", 1), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key3", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            AssertExtensions.WaitUntil(
                () => thread1.State == ThreadState.RUNNING && thread1.ActiveTasks.Count() == 2,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            Assert.AreEqual(2, thread1.ActiveTasks.Count());

            AssertExtensions.WaitUntil(
                () => thread1.ActiveTasks.ToList()[0].State == TaskState.RUNNING &&
                      thread1.ActiveTasks.ToList()[1].State == TaskState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var storeThreadTask1 =
                thread1.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;
            var storeThreadTask2 =
                thread1.ActiveTasks.ToList()[1].GetStore("store") as ITimestampedKeyValueStore<string, string>;

            Assert.IsNotNull(storeThreadTask1);
            Assert.IsNotNull(storeThreadTask2);

            bool task0Part0 = thread1.ActiveTasks.ToList()[0].Id.Partition == 0;

            AssertExtensions.WaitUntil(
                () => storeThreadTask1.All().ToList().Count == (task0Part0 ? 1 : 2),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(20));

            AssertExtensions.WaitUntil(
                () => storeThreadTask2.All().ToList().Count == (task0Part0 ? 2 : 1),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(20));

            var totalItemsSt10 = storeThreadTask1.All().ToList();
            var totalItemsSt11 = storeThreadTask2.All().ToList();

            Assert.AreEqual((task0Part0 ? 1 : 2), totalItemsSt10.Count);
            Assert.AreEqual((task0Part0 ? 2 : 1), totalItemsSt11.Count);
        }

        [Test]
        public void StreamThreadRestorationPhaseStartSmallDifferent()
        {
            var producerConfig = config.Clone();
            producerConfig.ApplicationId = "produce-test";

            var serdes = new StringSerDes();
            var producer = mockKafkaSupplier.GetProducer(producerConfig.ToProducerConfig());

            thread1.Start(token1.Token);
            Thread.Sleep(25);
            thread2.Start(token2.Token);

            producer.Produce(new TopicPartition("topic", 0), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });
            producer.Produce(new TopicPartition("topic", 1), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key2", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            AssertExtensions.WaitUntil(
                () => thread1.State == ThreadState.RUNNING &&
                      thread2.State == ThreadState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            // 2 CONSUMER FOR THE SAME GROUP ID => TOPIC WITH 2 PARTITIONS
            Assert.AreEqual(1, thread1.ActiveTasks.Count());
            Assert.AreEqual(1, thread2.ActiveTasks.Count());

            AssertExtensions.WaitUntil(
                () => thread1.ActiveTasks.ToList()[0].State == TaskState.RUNNING &&
                      thread2.ActiveTasks.ToList()[0].State == TaskState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var storeThread1 =
                thread1.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;
            var storeThread2 =
                thread2.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;

            Assert.IsNotNull(storeThread1);
            Assert.IsNotNull(storeThread2);

            AssertExtensions.WaitUntil(
                () => storeThread1.All().ToList().Count == 1,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            AssertExtensions.WaitUntil(
                () => storeThread2.All().ToList().Count == 1,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var totalItemsSt1 = storeThread1.All().ToList();
            var totalItemsSt2 = storeThread2.All().ToList();

            Assert.AreEqual(1, totalItemsSt1.Count);
            Assert.AreEqual(1, totalItemsSt2.Count);

            // Thread2 closed, partitions assigned from thread2 rebalance to thread1
            // Thread1 need to restore state store
            token2.Cancel();
            thread2.Dispose();

            thread2Disposed = true;

            producer.Produce(new TopicPartition("topic", 1), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key3", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            AssertExtensions.WaitUntil(
                () => thread1.State == ThreadState.RUNNING && thread1.ActiveTasks.Count() == 2,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            Assert.AreEqual(2, thread1.ActiveTasks.Count());

            AssertExtensions.WaitUntil(
                () => thread1.ActiveTasks.ToList()[0].State == TaskState.RUNNING &&
                      thread1.ActiveTasks.ToList()[1].State == TaskState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var storeThreadTask1 =
                thread1.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;
            var storeThreadTask2 =
                thread1.ActiveTasks.ToList()[1].GetStore("store") as ITimestampedKeyValueStore<string, string>;

            Assert.IsNotNull(storeThreadTask1);
            Assert.IsNotNull(storeThreadTask2);

            bool task0Part0 = thread1.ActiveTasks.ToList()[0].Id.Partition == 0;

            AssertExtensions.WaitUntil(
                () => storeThreadTask1.All().ToList().Count == (task0Part0 ? 1 : 2),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(20));

            AssertExtensions.WaitUntil(
                () => storeThreadTask2.All().ToList().Count == (task0Part0 ? 2 : 1),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(20));

            var totalItemsSt10 = storeThreadTask1.All().ToList();
            var totalItemsSt11 = storeThreadTask2.All().ToList();

            Assert.AreEqual((task0Part0 ? 1 : 2), totalItemsSt10.Count);
            Assert.AreEqual((task0Part0 ? 2 : 1), totalItemsSt11.Count);
        }

        [Test]
        public void StreamThreadRestorationPhaseStartParallel()
        {
            var producerConfig = config.Clone();
            producerConfig.ApplicationId = "produce-test";

            var serdes = new StringSerDes();
            var producer = mockKafkaSupplier.GetProducer(producerConfig.ToProducerConfig());

            thread1.Start(token1.Token);
            thread2.Start(token2.Token);

            producer.Produce(new TopicPartition("topic", 0), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });
            producer.Produce(new TopicPartition("topic", 1), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key2", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            AssertExtensions.WaitUntil(
                () => thread1.State == ThreadState.RUNNING &&
                      thread2.State == ThreadState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            // 2 CONSUMER FOR THE SAME GROUP ID => TOPIC WITH 2 PARTITIONS
            Assert.AreEqual(1, thread1.ActiveTasks.Count());
            Assert.AreEqual(1, thread2.ActiveTasks.Count());

            AssertExtensions.WaitUntil(
                () => thread1.ActiveTasks.ToList()[0].State == TaskState.RUNNING &&
                      thread2.ActiveTasks.ToList()[0].State == TaskState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var storeThread1 =
                thread1.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;
            var storeThread2 =
                thread2.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;

            Assert.IsNotNull(storeThread1);
            Assert.IsNotNull(storeThread2);

            AssertExtensions.WaitUntil(
                () => storeThread1.All().ToList().Count == 1,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            AssertExtensions.WaitUntil(
                () => storeThread2.All().ToList().Count == 1,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var totalItemsSt1 = storeThread1.All().ToList();
            var totalItemsSt2 = storeThread2.All().ToList();

            Assert.AreEqual(1, totalItemsSt1.Count);
            Assert.AreEqual(1, totalItemsSt2.Count);

            // Thread2 closed, partitions assigned from thread2 rebalance to thread1
            // Thread1 need to restore state store
            token2.Cancel();
            thread2.Dispose();

            thread2Disposed = true;

            producer.Produce(new TopicPartition("topic", 1), new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key3", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            AssertExtensions.WaitUntil(
                () => thread1.State == ThreadState.RUNNING && thread1.ActiveTasks.Count() == 2,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            Assert.AreEqual(2, thread1.ActiveTasks.Count());

            AssertExtensions.WaitUntil(
                () => thread1.ActiveTasks.ToList()[0].State == TaskState.RUNNING &&
                      thread1.ActiveTasks.ToList()[1].State == TaskState.RUNNING,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20));

            var storeThreadTask1 =
                thread1.ActiveTasks.ToList()[0].GetStore("store") as ITimestampedKeyValueStore<string, string>;
            var storeThreadTask2 =
                thread1.ActiveTasks.ToList()[1].GetStore("store") as ITimestampedKeyValueStore<string, string>;

            Assert.IsNotNull(storeThreadTask1);
            Assert.IsNotNull(storeThreadTask2);

            bool task0Part0 = thread1.ActiveTasks.ToList()[0].Id.Partition == 0;

            AssertExtensions.WaitUntil(
                () => storeThreadTask1.All().ToList().Count == (task0Part0 ? 1 : 2),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(20));

            AssertExtensions.WaitUntil(
                () => storeThreadTask2.All().ToList().Count == (task0Part0 ? 2 : 1),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(20));

            var totalItemsSt10 = storeThreadTask1.All().ToList();
            var totalItemsSt11 = storeThreadTask2.All().ToList();

            Assert.AreEqual((task0Part0 ? 1 : 2), totalItemsSt10.Count);
            Assert.AreEqual((task0Part0 ? 2 : 1), totalItemsSt11.Count);
        }

        #endregion
    }
}