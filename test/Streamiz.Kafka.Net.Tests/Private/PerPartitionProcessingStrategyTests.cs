using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Kafka;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class PerPartitionProcessingStrategyTests
    {
        private const string Topic = "test-topic";

        [Test]
        public void OffsetTracker_SequentialCompletion()
        {
            // Test that OffsetTracker properly tracks sequential offset completion
            var tracker = new OffsetTracker();
            var tp = new TopicPartition(Topic, 0);

            // Dispatch offsets 0, 1, 2
            for (int i = 0; i < 3; i++)
            {
                tracker.RecordDispatched(new TopicPartitionOffset(tp, i));
            }

            Assert.AreEqual(3, tracker.GetTotalInFlightCount());

            // Complete in order
            tracker.RecordCompleted(new TopicPartitionOffset(tp, 0));
            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(1, committable[0].Offset.Value); // Next offset to consume

            tracker.RecordCompleted(new TopicPartitionOffset(tp, 1));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(2, committable[0].Offset.Value);

            tracker.RecordCompleted(new TopicPartitionOffset(tp, 2));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(3, committable[0].Offset.Value);

            Assert.AreEqual(0, tracker.GetTotalInFlightCount());
        }

        [Test]
        public void OffsetTracker_OutOfOrderCompletion()
        {
            // Test that out-of-order completion only commits sequential offsets
            var tracker = new OffsetTracker();
            var tp = new TopicPartition(Topic, 0);

            // Dispatch offsets 0, 1, 2, 3, 4
            for (int i = 0; i < 5; i++)
            {
                tracker.RecordDispatched(new TopicPartitionOffset(tp, i));
            }

            // Complete out of order: 2, 4
            tracker.RecordCompleted(new TopicPartitionOffset(tp, 2));
            tracker.RecordCompleted(new TopicPartitionOffset(tp, 4));

            // Should not be able to commit anything yet (0 and 1 still in-flight)
            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.IsEmpty(committable);

            // Complete offset 0
            tracker.RecordCompleted(new TopicPartitionOffset(tp, 0));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(1, committable[0].Offset.Value); // Can only commit 1 (offset 1 still in-flight)

            // Complete offset 1
            tracker.RecordCompleted(new TopicPartitionOffset(tp, 1));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(3, committable[0].Offset.Value); // Can commit 3 (0,1,2 done, 3 still in-flight)

            // Complete offset 3
            tracker.RecordCompleted(new TopicPartitionOffset(tp, 3));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(5, committable[0].Offset.Value); // All done
        }

        [Test]
        public void OffsetTracker_MultiplePartitions()
        {
            // Test that multiple partitions are tracked independently
            var tracker = new OffsetTracker();
            var tp0 = new TopicPartition(Topic, 0);
            var tp1 = new TopicPartition(Topic, 1);

            // Dispatch to both partitions
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 0));
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 1));
            tracker.RecordDispatched(new TopicPartitionOffset(tp1, 0));
            tracker.RecordDispatched(new TopicPartitionOffset(tp1, 1));

            Assert.AreEqual(4, tracker.GetTotalInFlightCount());
            Assert.AreEqual(2, tracker.GetInFlightCount(tp0));
            Assert.AreEqual(2, tracker.GetInFlightCount(tp1));

            // Complete partition 0, offset 0
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 0));
            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(tp0, committable[0].TopicPartition);

            // Complete partition 1, offsets 0 and 1
            tracker.RecordCompleted(new TopicPartitionOffset(tp1, 0));
            tracker.RecordCompleted(new TopicPartitionOffset(tp1, 1));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(2, committable.Count);

            var p0Committable = committable.First(c => c.TopicPartition.Partition == 0);
            var p1Committable = committable.First(c => c.TopicPartition.Partition == 1);

            Assert.AreEqual(1, p0Committable.Offset.Value);
            Assert.AreEqual(2, p1Committable.Offset.Value);
        }

        [Test]
        public void ParallelProcessingConfig_Validation()
        {
            // Test config validation
            var config = new ParallelProcessingConfig
            {
                Mode = ParallelProcessingMode.PER_PARTITION,
                MaxConcurrency = 4,
                MaxQueuedRecords = 1000
            };

            Assert.DoesNotThrow(() => config.Validate());

            // Invalid concurrency
            config.MaxConcurrency = 0;
            Assert.Throws<ArgumentException>(() => config.Validate());

            config.MaxConcurrency = 4;

            // Invalid queue size
            config.MaxQueuedRecords = -1;
            Assert.Throws<ArgumentException>(() => config.Validate());

            config.MaxQueuedRecords = 1000;

            // Sequential mode with concurrency > 1
            config.Mode = ParallelProcessingMode.SEQUENTIAL;
            config.MaxConcurrency = 2;
            Assert.Throws<ArgumentException>(() => config.Validate());
        }

        [Test]
        public void ParallelProcessingConfig_FactoryMethods()
        {
            // Test factory methods
            var sequential = ParallelProcessingConfig.Sequential();
            Assert.AreEqual(ParallelProcessingMode.SEQUENTIAL, sequential.Mode);
            Assert.AreEqual(1, sequential.MaxConcurrency);

            var perPartition = ParallelProcessingConfig.PerPartition();
            Assert.AreEqual(ParallelProcessingMode.PER_PARTITION, perPartition.Mode);
            Assert.AreEqual(Environment.ProcessorCount, perPartition.MaxConcurrency);

            var perPartition4 = ParallelProcessingConfig.PerPartition(4);
            Assert.AreEqual(4, perPartition4.MaxConcurrency);

            var perKey = ParallelProcessingConfig.PerKey();
            Assert.AreEqual(ParallelProcessingMode.PER_KEY, perKey.Mode);
            Assert.AreEqual(Environment.ProcessorCount * 2, perKey.MaxConcurrency);

            var unordered = ParallelProcessingConfig.Unordered();
            Assert.AreEqual(ParallelProcessingMode.UNORDERED, unordered.Mode);
            Assert.AreEqual(Environment.ProcessorCount * 4, unordered.MaxConcurrency);
        }

        [Test]
        public void RecordWorkItem_Lifecycle()
        {
            // Test work item lifecycle
            var record = new ConsumeResult<byte[], byte[]>
            {
                Topic = Topic,
                Partition = 0,
                Offset = 10,
                Message = new Message<byte[], byte[]>
                {
                    Key = new byte[] { 1, 2, 3 },
                    Value = new byte[] { 4, 5, 6 }
                }
            };

            var workItem = new RecordWorkItem(record);

            Assert.AreEqual(record, workItem.Record);
            Assert.AreEqual(0, workItem.RetryCount);
            Assert.IsFalse(workItem.CompletionSource.Task.IsCompleted);

            // Complete successfully
            workItem.Complete();
            Assert.IsTrue(workItem.CompletionSource.Task.IsCompleted);
            Assert.AreEqual(ProcessingResult.Success, workItem.CompletionSource.Task.Result);
        }

        [Test]
        public void RecordWorkItem_Failure()
        {
            var record = new ConsumeResult<byte[], byte[]>
            {
                Topic = Topic,
                Partition = 0,
                Offset = 10,
                Message = new Message<byte[], byte[]>()
            };

            var workItem = new RecordWorkItem(record);
            var exception = new Exception("Test failure");

            workItem.Fail(exception);

            Assert.IsTrue(workItem.CompletionSource.Task.IsFaulted);
            Assert.AreEqual(exception, workItem.CompletionSource.Task.Exception.InnerException);
        }

        [Test]
        public void RecordWorkItem_Cancel()
        {
            var record = new ConsumeResult<byte[], byte[]>
            {
                Topic = Topic,
                Partition = 0,
                Offset = 10,
                Message = new Message<byte[], byte[]>()
            };

            var workItem = new RecordWorkItem(record);
            workItem.Cancel();

            Assert.IsTrue(workItem.CompletionSource.Task.IsCanceled);
        }
    }
}
