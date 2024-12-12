using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Globalization;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.State.Internal;
using System.IO;
using System.Threading.Tasks.Sources;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StreamTaskTests
    {
         [Test]
        public void StreamCloseRunning()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v, _) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();
            TaskId id = new TaskId {Id = 0, Partition = 0};
            var processorTopology = topology.Builder.BuildTopology(id);

            var supplier = new SyncKafkaSupplier();
            var streamsProducer = new StreamsProducer(
                config,
                "thread-0",
                Guid.NewGuid(),
                supplier,
                "log-prefix");
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);


            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                new List<TopicPartition> {part},
                processorTopology,
                consumer,
                config,
                supplier,
                streamsProducer,
                new MockChangelogRegister()
                , new StreamMetricsRegistry());
            task.InitializeStateStores();
            task.InitializeTopology();
            task.RestorationIfNeeded();
            task.CompleteRestoration();

            Assert.Throws<IllegalStateException>(() => task.Close(false));
        }
        
        [Test]
        public void StreamCloseAlreadyClosed()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v, _) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();
            TaskId id = new TaskId {Id = 0, Partition = 0};
            var processorTopology = topology.Builder.BuildTopology(id);

            var supplier = new SyncKafkaSupplier();
            var streamsProducer = new StreamsProducer(
                config,
                "thread-0",
                Guid.NewGuid(),
                supplier,
                "log-prefix");
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);


            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                new List<TopicPartition> {part},
                processorTopology,
                consumer,
                config,
                supplier,
                streamsProducer,
                new MockChangelogRegister()
                , new StreamMetricsRegistry());
            task.InitializeStateStores();
            task.InitializeTopology();
            task.RestorationIfNeeded();
            task.CompleteRestoration();

            task.Suspend();
            task.Close(false);
            task.Close(false);
        }

        
        [Test]
        public void StreamTaskRunning()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v, _) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();
            TaskId id = new TaskId {Id = 0, Partition = 0};
            var processorTopology = topology.Builder.BuildTopology(id);

            var supplier = new SyncKafkaSupplier();
            var streamsProducer = new StreamsProducer(
                config,
                "thread-0",
                Guid.NewGuid(),
                supplier,
                "log-prefix");
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);


            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                new List<TopicPartition> {part},
                processorTopology,
                consumer,
                config,
                supplier,
                streamsProducer,
                new MockChangelogRegister()
                , new StreamMetricsRegistry());
            task.InitializeStateStores();
            task.InitializeTopology();
            task.RestorationIfNeeded();
            task.CompleteRestoration();

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
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.PrepareCommit();
                task.PostCommit(false);
            }

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

            task.Suspend();
            task.Close(false);
        }

        [Test]
        public void StreamTaskSuspendRecreate()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Table<string, string>("topic", InMemory.As<string, string>("store").WithLoggingDisabled())
                .MapValues((k, v, _) => v.ToUpper())
                .ToStream()
                .To("topic2");


            TaskId id = new TaskId {Id = 0, Partition = 0};
            var topology = builder.Build();
            var processorTopology = topology.Builder.BuildTopology(id);

            var supplier = new SyncKafkaSupplier();
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            var streamsProducer = new StreamsProducer(
                config,
                "thread-0",
                Guid.NewGuid(),
                supplier,
                "log-prefix");
            
            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                new List<TopicPartition> {part},
                processorTopology,
                consumer,
                config,
                supplier,
                streamsProducer,
                new MockChangelogRegister()
                , new StreamMetricsRegistry());
            task.InitializeStateStores();
            task.InitializeTopology();
            task.RestorationIfNeeded();
            task.CompleteRestoration();

            List<ConsumeResult<byte[], byte[]>> messages = new List<ConsumeResult<byte[], byte[]>>();
            int offset = 0;
            for (int i = 0; i < 5; ++i)
            {
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
            }

            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess(DateTime.Now.GetMilliseconds()));

            while (task.CanProcess(DateTime.Now.GetMilliseconds()))
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.PrepareCommit();
                task.PostCommit(false);
            }

            Assert.IsNotNull(task.GetStore("store"));
            task.Suspend();
            task.Close(false);
            Assert.IsNull(task.GetStore("store"));
            
            //recreate task
            task = new StreamTask(
                "thread-0",
                id,
                new List<TopicPartition> {part},
                processorTopology,
                consumer,
                config,
                supplier,
                streamsProducer,
                new MockChangelogRegister()
                , new StreamMetricsRegistry());
            task.InitializeStateStores();
            task.InitializeTopology();
            task.RestorationIfNeeded();
            task.CompleteRestoration();

            Assert.IsNotNull(task.GetStore("store"));
            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess(DateTime.Now.GetMilliseconds()));

            while (task.CanProcess(DateTime.Now.GetMilliseconds()))
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.PrepareCommit();
                task.PostCommit(false);
            }

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

            Assert.AreEqual(10, results.Count);

            task.Suspend();
            task.Close(false);
        }

        [Test]
        public void StreamTaskWrittingCheckpoint()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.StateDir = Path.Combine(".", Guid.NewGuid().ToString());

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            var table = builder.Table("topic", RocksDb.As<string, string>("store").WithLoggingEnabled());

            TaskId id = new TaskId {Id = 0, Partition = 0};
            var topology = builder.Build();
            topology.Builder.RewriteTopology(config);

            var processorTopology = topology.Builder.BuildTopology(id);

            var supplier = new SyncKafkaSupplier();
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            var streamsProducer = new StreamsProducer(
                config,
                "thread-0",
                Guid.NewGuid(),
                supplier,
                "log-prefix");
            
            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                new List<TopicPartition> {part},
                processorTopology,
                consumer,
                config,
                supplier,
                streamsProducer,
                new MockChangelogRegister()
                , new StreamMetricsRegistry());
            task.InitializeStateStores();
            task.InitializeTopology();
            task.RestorationIfNeeded();
            task.CompleteRestoration();

            List<ConsumeResult<byte[], byte[]>> messages = new List<ConsumeResult<byte[], byte[]>>();
            int offset = 0;
            for (int i = 0; i < 5; ++i)
            {
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
            }

            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess(DateTime.Now.GetMilliseconds()));

            while (task.CanProcess(DateTime.Now.GetMilliseconds()))
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.PrepareCommit();
                task.PostCommit(true);
            }

            messages = new List<ConsumeResult<byte[], byte[]>>();
            for (int i = 0; i < StateManagerTools.OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT + 10; ++i)
            {
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
            }

            task.AddRecords(messages);

            while (task.CanProcess(DateTime.Now.GetMilliseconds()))
            {
                Assert.IsTrue(task.Process());
            }

            task.PrepareCommit();
            task.PostCommit(false);

            var lines = File.ReadAllLines(Path.Combine(config.StateDir, config.ApplicationId, "0-0", ".checkpoint"));
            Assert.AreEqual(3, lines.Length);
            Assert.AreEqual("test-app-store-changelog 0 10015", lines[2]);

            task.Suspend();
            task.Close(false);

            Directory.Delete(config.StateDir, true);
        }
    }
}