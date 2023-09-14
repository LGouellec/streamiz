using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using System.IO;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class TaskManagerTests
    {
        [Test]
        public void StandardWorkflowTaskManager()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1),
                    new TopicPartition("topic", 2),
                    new TopicPartition("topic", 3),
                });
            taskManager.TryToCompleteRestoration();

            Assert.AreEqual(4, taskManager.ActiveTasks.Count());
            for (int i = 0; i < 4; ++i)
            {
                var task = taskManager.ActiveTaskFor(new TopicPartition("topic", i));
                Assert.IsNotNull(task);
                Assert.AreEqual("test-app", task.ApplicationId);
                Assert.IsFalse(task.CanProcess(DateTime.Now.GetMilliseconds()));
                Assert.IsFalse(task.CommitNeeded);
                Assert.IsFalse(task.HasStateStores);
            }

            // Revoked 2 partitions
            taskManager.RevokeTasks(new List<TopicPartition>
            {
                new TopicPartition("topic", 2),
                new TopicPartition("topic", 3),
            });
            Assert.AreEqual(2, taskManager.ActiveTasks.Count());
            for (int i = 0; i < 2; ++i)
            {
                var task = taskManager.ActiveTaskFor(new TopicPartition("topic", i));
                Assert.IsNotNull(task);
                Assert.AreEqual("test-app", task.ApplicationId);
                Assert.IsFalse(task.CanProcess(DateTime.Now.GetMilliseconds()));
                Assert.IsFalse(task.CommitNeeded);
                Assert.IsFalse(task.HasStateStores);
            }

            var taskFailed = taskManager.ActiveTaskFor(new TopicPartition("topic", 2));
            Assert.IsNull(taskFailed);

            taskManager.Close();
            Assert.AreEqual(0, taskManager.ActiveTasks.Count());
        }

        [Test]
        public void TaskManagerCommitWithoutCommitNeeed()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1),
                    new TopicPartition("topic", 2),
                    new TopicPartition("topic", 3),
                });
            taskManager.TryToCompleteRestoration();

            Assert.AreEqual(4, taskManager.ActiveTasks.Count());
            Assert.AreEqual(0, taskManager.CommitAll());
            taskManager.Close();
        }

        [Test]
        public void TaskManagerCommit()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1),
                    new TopicPartition("topic", 2),
                    new TopicPartition("topic", 3),
                });
            taskManager.TryToCompleteRestoration();

            Assert.AreEqual(4, taskManager.ActiveTasks.Count());

            var part = new TopicPartition("topic", 0);
            var task = taskManager.ActiveTaskFor(part);
            List<ConsumeResult<byte[], byte[]>> messages = new List<ConsumeResult<byte[], byte[]>>();
            int offset = 0;
            for (int i = 0; i < 5; ++i)
                messages.Add(
                    new ConsumeResult<byte[], byte[]>
                    {
                        Message = new Message<byte[], byte[]>
                        {
                            Key = serdes.Serialize($"key{i + 1}", new SerializationContext()),
                            Value = serdes.Serialize($"value{i + 1}", new SerializationContext())
                        },
                        TopicPartitionOffset = new TopicPartitionOffset(part, offset++)
                    });

            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess(DateTime.Now.GetMilliseconds()));

            while (task.CanProcess(DateTime.Now.GetMilliseconds()))
                Assert.IsTrue(task.Process());

            // ONLY ONE TASK HAVE BEEN RECORDS
            Assert.AreEqual(1, taskManager.CommitAll());

            // CHECK IN TOPIC topic2
            consumer.Subscribe("topic2");
            List<ConsumeResult<byte[], byte[]>> results = new List<ConsumeResult<byte[], byte[]>>();
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);

                if (result != null)
                {
                    results.Add(result);
                    consumer.Commit(result);
                }
            } while (result != null);

            Assert.AreEqual(5, results.Count);
            for (int i = 0; i < 5; ++i)
            {
                Assert.AreEqual($"KEY{i + 1}", serdes.Deserialize(results[i].Message.Key, new SerializationContext()));
                Assert.AreEqual($"VALUE{i + 1}",
                    serdes.Deserialize(results[i].Message.Value, new SerializationContext()));
            }

            // NO RECORD IN THIS TASKS
            part = new TopicPartition("topic", 2);
            task = taskManager.ActiveTaskFor(part);
            Assert.IsFalse(task.CanProcess(DateTime.Now.GetMilliseconds()));
            Assert.IsFalse(task.Process());

            taskManager.Close();
        }

        [Test]
        public void TaskManagerReassignedRevokedPartitions()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1)
                });

            taskManager.RevokeTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 1)
                });

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1)
                });
            taskManager.TryToCompleteRestoration();

            Assert.AreEqual(2, taskManager.ActiveTasks.Count());
            taskManager.Close();
        }

        [Test]
        public void TaskManagerAssignedUnknownPartitions()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1)
                });

            taskManager.RevokeTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 1)
                });

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1),
                    new TopicPartition("topic", 2)
                });

            taskManager.TryToCompleteRestoration();

            Assert.AreEqual(3, taskManager.ActiveTasks.Count());
            taskManager.Close();
        }

        [Test]
        public void TaskManagerRestorationChangelogNotPersistent()
        {
            TaskManagerRestorationChangelog();
        }

        [Test]
        public void TaskManagerRestorationChangelogPersistent()
        {
            TaskManagerRestorationChangelog(true);
        }


        private void TaskManagerRestorationChangelog(bool persistenStateStore = false)
        {
            var stateDir = Path.Combine(".", Guid.NewGuid().ToString());
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-restoration-changelog-app";
            config.StateDir = stateDir;

            var builder = new StreamBuilder();
            builder.Table("topic",
                persistenStateStore
                    ? RocksDb.As<string, string>("store").WithLoggingEnabled(null)
                    : InMemory.As<string, string>("store").WithLoggingEnabled(null));

            var serdes = new StringSerDes();

            var topology = builder.Build();
            topology.Builder.RewriteTopology(config);

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            var part = new TopicPartition("topic", 0);

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    part
                });

            var task = taskManager.ActiveTaskFor(part);

            IDictionary<TaskId, ITask> tasks = new Dictionary<TaskId, ITask>();
            tasks.Add(task.Id, task);

            taskManager.TryToCompleteRestoration();
            storeChangelogReader.Restore();
            Assert.IsTrue(taskManager.TryToCompleteRestoration());


            List<ConsumeResult<byte[], byte[]>> messages = new List<ConsumeResult<byte[], byte[]>>();
            int offset = 0;
            for (int i = 0; i < 5; ++i)
                messages.Add(
                    new ConsumeResult<byte[], byte[]>
                    {
                        Message = new Message<byte[], byte[]>
                        {
                            Key = serdes.Serialize($"key{i + 1}", new SerializationContext()),
                            Value = serdes.Serialize($"value{i + 1}", new SerializationContext())
                        },
                        TopicPartitionOffset = new TopicPartitionOffset(part, offset++)
                    });

            task.AddRecords(messages);

            // Process messages
            while (task.CanProcess(DateTime.Now.GetMilliseconds()))
                Assert.IsTrue(task.Process());

            taskManager.CommitAll();

            // Simulate Close + new open 
            taskManager.Close();

            restoreConsumer.Resume(new TopicPartition("test-restoration-changelog-app-store-changelog", 0).ToSingle());

            taskManager.CreateTasks(
                new List<TopicPartition>
                {
                    part
                });

            task = taskManager.ActiveTaskFor(part);
            tasks = new Dictionary<TaskId, ITask>();
            tasks.Add(task.Id, task);

            Assert.IsFalse(taskManager.TryToCompleteRestoration());
            storeChangelogReader.Restore();
            Assert.IsTrue(taskManager.TryToCompleteRestoration());

            var store = task.GetStore("store");
            var items = (store as ITimestampedKeyValueStore<string, string>).All().ToList();

            Assert.AreEqual(5, items.Count);

            taskManager.Close();

            if (persistenStateStore)
                Directory.Delete(stateDir, true);
        }
    }
}