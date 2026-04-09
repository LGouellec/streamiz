using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Tracks in-flight and completed offsets to determine which offsets can be safely committed.
    /// Ensures at-least-once semantics by only returning sequential completed offsets for commit.
    /// Thread-safe for concurrent access by multiple workers.
    /// </summary>
    internal class OffsetTracker
    {
        /// <summary>
        /// State for a single topic-partition.
        /// </summary>
        private class PartitionOffsetState
        {
            /// <summary>
            /// Lock for synchronizing access to this partition's state.
            /// </summary>
            public readonly object Lock = new object();

            /// <summary>
            /// Set of offsets currently in-flight (dispatched but not completed).
            /// </summary>
            public SortedSet<long> InFlight { get; } = new SortedSet<long>();

            /// <summary>
            /// Highest offset that has been completed and all preceding offsets are also completed.
            /// This is the offset that can be safely committed.
            /// Initial value is -1 (no offsets completed yet).
            /// </summary>
            public long HighestSequentialCompleted { get; set; } = -1;

            /// <summary>
            /// Highest offset that has been dispatched (polled from Kafka).
            /// Used for tracking and metrics.
            /// </summary>
            public long HighestDispatched { get; set; } = -1;
        }

        private readonly ConcurrentDictionary<TopicPartition, PartitionOffsetState> _partitionStates;

        /// <summary>
        /// Creates a new offset tracker.
        /// </summary>
        public OffsetTracker()
        {
            _partitionStates = new ConcurrentDictionary<TopicPartition, PartitionOffsetState>();
        }

        /// <summary>
        /// Records that an offset has been dispatched for processing.
        /// </summary>
        /// <param name="tpo">The topic-partition-offset that was dispatched</param>
        public void RecordDispatched(TopicPartitionOffset tpo)
        {
            if (tpo == null)
                throw new ArgumentNullException(nameof(tpo));

            var state = GetOrCreateState(tpo.TopicPartition);
            lock (state.Lock)
            {
                long offset = tpo.Offset.Value;
                state.InFlight.Add(offset);

                if (offset > state.HighestDispatched)
                    state.HighestDispatched = offset;
            }
        }

        /// <summary>
        /// Records that an offset has been completed (successfully processed).
        /// Updates the highest sequential completed offset.
        /// </summary>
        /// <param name="tpo">The topic-partition-offset that completed</param>
        public void RecordCompleted(TopicPartitionOffset tpo)
        {
            if (tpo == null)
                throw new ArgumentNullException(nameof(tpo));

            var state = GetOrCreateState(tpo.TopicPartition);
            lock (state.Lock)
            {
                long offset = tpo.Offset.Value;

                // Remove from in-flight
                if (!state.InFlight.Remove(offset))
                {
                    // Offset was not in-flight - this could happen if we're completing
                    // an offset that was already completed or never dispatched
                    return;
                }

                // Update highest sequential completed offset
                // We can only commit up to the highest offset where all preceding offsets are also completed
                long expectedOffset = state.HighestSequentialCompleted + 1;

                while (expectedOffset <= state.HighestDispatched && !state.InFlight.Contains(expectedOffset))
                {
                    state.HighestSequentialCompleted = expectedOffset;
                    expectedOffset++;
                }
            }
        }

        /// <summary>
        /// Gets the offsets that can be safely committed to Kafka.
        /// Only returns offsets where all preceding offsets have been completed.
        /// The returned offset is the "next offset to consume" (completed + 1) as per Kafka commit semantics.
        /// </summary>
        /// <returns>Collection of committable offsets</returns>
        public IEnumerable<TopicPartitionOffset> GetCommittableOffsets()
        {
            var committableOffsets = new List<TopicPartitionOffset>();

            foreach (var kv in _partitionStates)
            {
                var topicPartition = kv.Key;
                var state = kv.Value;

                lock (state.Lock)
                {
                    if (state.HighestSequentialCompleted >= 0)
                    {
                        // Kafka commit semantics: commit the "next offset to consume"
                        // which is the last completed offset + 1
                        long commitOffset = state.HighestSequentialCompleted + 1;
                        committableOffsets.Add(new TopicPartitionOffset(topicPartition, commitOffset));
                    }
                }
            }

            return committableOffsets;
        }

        /// <summary>
        /// Clears all tracked offsets for the given topic-partition.
        /// Used after successful commit or during rebalance.
        /// </summary>
        /// <param name="topicPartition">The topic-partition to clear</param>
        public void ClearPartition(TopicPartition topicPartition)
        {
            if (topicPartition == null)
                throw new ArgumentNullException(nameof(topicPartition));

            if (_partitionStates.TryGetValue(topicPartition, out var state))
            {
                lock (state.Lock)
                {
                    state.InFlight.Clear();
                    state.HighestSequentialCompleted = -1;
                    state.HighestDispatched = -1;
                }
            }
        }

        /// <summary>
        /// Clears all tracked offsets.
        /// Used during shutdown or reset.
        /// </summary>
        public void ClearAll()
        {
            foreach (var tp in _partitionStates.Keys.ToList())
            {
                ClearPartition(tp);
            }
            _partitionStates.Clear();
        }

        /// <summary>
        /// Gets the number of in-flight offsets for a specific topic-partition.
        /// </summary>
        /// <param name="topicPartition">The topic-partition to query</param>
        /// <returns>Number of in-flight offsets</returns>
        public int GetInFlightCount(TopicPartition topicPartition)
        {
            if (topicPartition == null)
                throw new ArgumentNullException(nameof(topicPartition));

            if (_partitionStates.TryGetValue(topicPartition, out var state))
            {
                lock (state.Lock)
                {
                    return state.InFlight.Count;
                }
            }

            return 0;
        }

        /// <summary>
        /// Gets the total number of in-flight offsets across all partitions.
        /// </summary>
        /// <returns>Total in-flight count</returns>
        public int GetTotalInFlightCount()
        {
            int total = 0;
            foreach (var state in _partitionStates.Values)
            {
                lock (state.Lock)
                {
                    total += state.InFlight.Count;
                }
            }
            return total;
        }

        /// <summary>
        /// Gets the offset lag for a topic-partition (highest dispatched - highest sequential completed).
        /// Indicates how many offsets are waiting to be completed sequentially.
        /// </summary>
        /// <param name="topicPartition">The topic-partition to query</param>
        /// <returns>Offset lag, or -1 if partition not tracked</returns>
        public long GetOffsetLag(TopicPartition topicPartition)
        {
            if (topicPartition == null)
                throw new ArgumentNullException(nameof(topicPartition));

            if (_partitionStates.TryGetValue(topicPartition, out var state))
            {
                lock (state.Lock)
                {
                    if (state.HighestDispatched < 0)
                        return 0;

                    return state.HighestDispatched - state.HighestSequentialCompleted;
                }
            }

            return -1;
        }

        /// <summary>
        /// Gets or creates the state for a topic-partition.
        /// </summary>
        private PartitionOffsetState GetOrCreateState(TopicPartition topicPartition)
        {
            return _partitionStates.GetOrAdd(topicPartition, _ => new PartitionOffsetState());
        }
    }
}
