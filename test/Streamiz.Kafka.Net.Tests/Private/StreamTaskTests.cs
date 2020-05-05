using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StreamTaskTests
    {
        [Test]
        public void StreamTaskWithEXACTLY_ONCE()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();
            var processorTopology = topology.Builder.BuildTopology("topic");

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            TaskId id = new TaskId { Id = 1, Topic = "topic", Partition = 0 };
            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                part,
                processorTopology,
                consumer,
                config,
                supplier,
                null);
            task.GroupMetadata = consumer as SyncConsumer;
            task.InitializeStateStores();
            task.InitializeTopology();

            List <ConsumeResult<byte[], byte[]>> messages = new List<ConsumeResult<byte[], byte[]>>();
            int offset = 0;
            for (int i = 0; i < 5; ++i)
                messages.Add(
                    new ConsumeResult<byte[], byte[]>
                    {
                        Message = new Message<byte[], byte[]>
                        {
                            Key = serdes.Serialize($"key{i + 1}"),
                            Value = serdes.Serialize($"value{i + 1}")
                        },
                        TopicPartitionOffset = new TopicPartitionOffset(part, offset++)
                    });

            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess);

            while (task.CanProcess)
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.Commit();
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
                Assert.AreEqual($"KEY{i + 1}", serdes.Deserialize(results[i].Message.Key));
                Assert.AreEqual($"VALUE{i + 1}", serdes.Deserialize(results[i].Message.Value));
            }

            task.Close();
        }

        [Test]
        public void StreamTaskSuspendResumeWithEXACTLY_ONCE()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();
            var processorTopology = topology.Builder.BuildTopology("topic");

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            TaskId id = new TaskId { Id = 1, Topic = "topic", Partition = 0 };
            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                part,
                processorTopology,
                consumer,
                config,
                supplier,
                null);
            task.GroupMetadata = consumer as SyncConsumer;
            task.InitializeStateStores();
            task.InitializeTopology();

            List<ConsumeResult<byte[], byte[]>> messages = new List<ConsumeResult<byte[], byte[]>>();
            int offset = 0;
            for (int i = 0; i < 5; ++i)
            {
                messages.Add(
                    new ConsumeResult<byte[], byte[]>
                    {
                        Message = new Message<byte[], byte[]>
                        {
                            Key = serdes.Serialize($"key{i + 1}"),
                            Value = serdes.Serialize($"value{i + 1}")
                        },
                        TopicPartitionOffset = new TopicPartitionOffset(part, offset++)
                    });
            }

            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess);

            while (task.CanProcess)
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.Commit();
            }

            task.Suspend();
            task.Resume();
            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess);

            while (task.CanProcess)
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.Commit();
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

            task.Close();
        }

        [Test]
        public void StreamTaskSuspendResume()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .To("topic2");

            var topology = builder.Build();
            var processorTopology = topology.Builder.BuildTopology("topic");

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            TaskId id = new TaskId { Id = 1, Topic = "topic", Partition = 0 };
            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                part,
                processorTopology,
                consumer,
                config,
                supplier,
                producer);
            task.GroupMetadata = consumer as SyncConsumer;
            task.InitializeStateStores();
            task.InitializeTopology();

            List<ConsumeResult<byte[], byte[]>> messages = new List<ConsumeResult<byte[], byte[]>>();
            int offset = 0;
            for (int i = 0; i < 5; ++i)
            {
                messages.Add(
                    new ConsumeResult<byte[], byte[]>
                    {
                        Message = new Message<byte[], byte[]>
                        {
                            Key = serdes.Serialize($"key{i+1}"),
                            Value = serdes.Serialize($"value{i + 1}")
                        },
                        TopicPartitionOffset = new TopicPartitionOffset(part, offset++)
                    });
            }

            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess);

            while (task.CanProcess)
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.Commit();
            }

            task.Suspend();
            task.Resume();
            task.AddRecords(messages);

            Assert.IsTrue(task.CanProcess);

            while (task.CanProcess)
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.Commit();
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

            task.Close();
        }

    }
}
