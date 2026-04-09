using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class OffsetTrackerTests
    {
        private const string Topic = "test-topic";
        private readonly TopicPartition tp0 = new TopicPartition(Topic, 0);
        private readonly TopicPartition tp1 = new TopicPartition(Topic, 1);

        [Test]
        public void InitialState_NoCommittableOffsets()
        {
            var tracker = new OffsetTracker();
            var committable = tracker.GetCommittableOffsets();

            Assert.IsEmpty(committable);
            Assert.AreEqual(0, tracker.GetTotalInFlightCount());
        }

        [Test]
        public void SingleOffset_DispatchedAndCompleted()
        {
            var tracker = new OffsetTracker();
            var tpo = new TopicPartitionOffset(tp0, 0);

            tracker.RecordDispatched(tpo);
            Assert.AreEqual(1, tracker.GetTotalInFlightCount());
            Assert.AreEqual(1, tracker.GetInFlightCount(tp0));

            tracker.RecordCompleted(tpo);
            Assert.AreEqual(0, tracker.GetTotalInFlightCount());

            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(tp0, committable[0].TopicPartition);
            Assert.AreEqual(1, committable[0].Offset.Value); // Kafka commit semantics: next offset to consume
        }

        [Test]
        public void SequentialOffsets_AllCompletedInOrder()
        {
            var tracker = new OffsetTracker();

            // Dispatch offsets 0, 1, 2
            for (int i = 0; i < 3; i++)
            {
                tracker.RecordDispatched(new TopicPartitionOffset(tp0, i));
            }

            Assert.AreEqual(3, tracker.GetInFlightCount(tp0));

            // Complete in order: 0, 1, 2
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 0));
            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(1, committable[0].Offset.Value); // Can commit up to offset 1 (next after 0)

            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 1));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(2, committable[0].Offset.Value); // Can commit up to offset 2 (next after 1)

            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 2));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(3, committable[0].Offset.Value); // Can commit up to offset 3 (next after 2)

            Assert.AreEqual(0, tracker.GetInFlightCount(tp0));
        }

        [Test]
        public void OutOfOrderCompletion_OnlySequentialOffsetCommittable()
        {
            var tracker = new OffsetTracker();

            // Dispatch offsets 0, 1, 2, 3, 4
            for (int i = 0; i < 5; i++)
            {
                tracker.RecordDispatched(new TopicPartitionOffset(tp0, i));
            }

            // Complete out of order: 2, 4, 0 (leaving 1 and 3 in-flight)
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 2));
            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.IsEmpty(committable, "Should not commit anything - offset 0 not yet completed");

            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 4));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.IsEmpty(committable, "Should still not commit anything - offset 0 still not completed");

            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 0));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(1, committable[0].Offset.Value, "Can only commit offset 1 - offset 1 is still in-flight");

            // Now complete offset 1
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 1));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(3, committable[0].Offset.Value, "Can commit offset 3 - offsets 0,1,2 completed but 3 still in-flight");

            // Finally complete offset 3
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 3));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(5, committable[0].Offset.Value, "Can commit offset 5 - all offsets 0,1,2,3,4 completed");
        }

        [Test]
        public void MultiplePartitions_TrackedIndependently()
        {
            var tracker = new OffsetTracker();

            // Dispatch to partition 0
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 0));
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 1));

            // Dispatch to partition 1
            tracker.RecordDispatched(new TopicPartitionOffset(tp1, 0));
            tracker.RecordDispatched(new TopicPartitionOffset(tp1, 1));

            Assert.AreEqual(2, tracker.GetInFlightCount(tp0));
            Assert.AreEqual(2, tracker.GetInFlightCount(tp1));
            Assert.AreEqual(4, tracker.GetTotalInFlightCount());

            // Complete partition 0 offset 0
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 0));
            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(tp0, committable[0].TopicPartition);
            Assert.AreEqual(1, committable[0].Offset.Value);

            // Complete partition 1 offsets 0 and 1
            tracker.RecordCompleted(new TopicPartitionOffset(tp1, 0));
            tracker.RecordCompleted(new TopicPartitionOffset(tp1, 1));
            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(2, committable.Count);

            var p0Committable = committable.First(c => c.TopicPartition.Partition == 0);
            var p1Committable = committable.First(c => c.TopicPartition.Partition == 1);

            Assert.AreEqual(1, p0Committable.Offset.Value, "Partition 0 still has offset 1 in-flight");
            Assert.AreEqual(2, p1Committable.Offset.Value, "Partition 1 completed both offsets");
        }

        [Test]
        public void ClearPartition_RemovesTracking()
        {
            var tracker = new OffsetTracker();

            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 0));
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 1));
            Assert.AreEqual(2, tracker.GetInFlightCount(tp0));

            tracker.ClearPartition(tp0);
            Assert.AreEqual(0, tracker.GetInFlightCount(tp0));

            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.IsEmpty(committable);
        }

        [Test]
        public void ClearAll_RemovesAllTracking()
        {
            var tracker = new OffsetTracker();

            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 0));
            tracker.RecordDispatched(new TopicPartitionOffset(tp1, 0));
            Assert.AreEqual(2, tracker.GetTotalInFlightCount());

            tracker.ClearAll();
            Assert.AreEqual(0, tracker.GetTotalInFlightCount());
            Assert.AreEqual(0, tracker.GetInFlightCount(tp0));
            Assert.AreEqual(0, tracker.GetInFlightCount(tp1));
        }

        [Test]
        public void OffsetLag_CalculatedCorrectly()
        {
            var tracker = new OffsetTracker();

            // Initially no lag
            Assert.AreEqual(-1, tracker.GetOffsetLag(tp0), "Should return -1 for untracked partition");

            // Dispatch offsets 0-4
            for (int i = 0; i < 5; i++)
            {
                tracker.RecordDispatched(new TopicPartitionOffset(tp0, i));
            }

            // Lag should be 5 (highest dispatched 4 - highest completed -1)
            Assert.AreEqual(5, tracker.GetOffsetLag(tp0));

            // Complete offsets 0 and 1
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 0));
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 1));

            // Lag should be 3 (highest dispatched 4 - highest completed 1)
            Assert.AreEqual(3, tracker.GetOffsetLag(tp0));

            // Complete remaining offsets
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 2));
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 3));
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 4));

            // Lag should be 0 (all completed)
            Assert.AreEqual(0, tracker.GetOffsetLag(tp0));
        }

        [Test]
        public void CompletingNonDispatchedOffset_Ignored()
        {
            var tracker = new OffsetTracker();

            // Complete an offset that was never dispatched
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 10));

            // Should have no effect
            Assert.AreEqual(0, tracker.GetTotalInFlightCount());
            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.IsEmpty(committable);
        }

        [Test]
        public void CompletingSameOffsetTwice_SecondIgnored()
        {
            var tracker = new OffsetTracker();

            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 0));
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 0));

            Assert.AreEqual(0, tracker.GetInFlightCount(tp0));

            // Try to complete again
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 0));

            // Should still be 0
            Assert.AreEqual(0, tracker.GetInFlightCount(tp0));

            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(1, committable[0].Offset.Value);
        }

        [Test]
        public void LargeGap_OnlySequentialCommittable()
        {
            var tracker = new OffsetTracker();

            // Dispatch 0, 1, 2, then a large gap to 100
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 0));
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 1));
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 2));
            tracker.RecordDispatched(new TopicPartitionOffset(tp0, 100));

            // Complete 0, 1, 100 (leaving 2 in-flight)
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 0));
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 1));
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 100));

            var committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(1, committable.Count);
            Assert.AreEqual(2, committable[0].Offset.Value, "Can only commit offset 2 - offset 2 still in-flight");

            // Complete offset 2
            tracker.RecordCompleted(new TopicPartitionOffset(tp0, 2));

            committable = tracker.GetCommittableOffsets().ToList();
            Assert.AreEqual(101, committable[0].Offset.Value, "Can now commit offset 101 - all dispatched offsets completed");
        }

        [Test]
        public void NullArguments_ThrowsException()
        {
            var tracker = new OffsetTracker();

            Assert.Throws<System.ArgumentNullException>(() => tracker.RecordDispatched(null));
            Assert.Throws<System.ArgumentNullException>(() => tracker.RecordCompleted(null));
            Assert.Throws<System.ArgumentNullException>(() => tracker.ClearPartition(null));
            Assert.Throws<System.ArgumentNullException>(() => tracker.GetInFlightCount(null));
            Assert.Throws<System.ArgumentNullException>(() => tracker.GetOffsetLag(null));
        }
    }
}
