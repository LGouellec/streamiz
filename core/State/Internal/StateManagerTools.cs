using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.State.Internal
{
    public static class StateManagerTools
    {
        static long OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT = 10_000L;

        static StateManagerTools()
        {
        }

        public static bool CheckpointNeed(bool force,
            IDictionary<TopicPartition, long> oldOffsetSnapshot,
            IDictionary<TopicPartition, long> newOffsetSnapshot)
        {
            if (force)
                return true;

            if (oldOffsetSnapshot == null)
                return false;

            long totalOffsetDelta = 0L;
            foreach(var kv in newOffsetSnapshot)
                totalOffsetDelta += kv.Value - (oldOffsetSnapshot.ContainsKey(kv.Key) ? oldOffsetSnapshot[kv.Key] : 0L);

            return totalOffsetDelta > OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT;
        }
    }
}
