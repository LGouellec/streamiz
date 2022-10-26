using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal static class StateManagerTools
    {
        public static long OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT = 10_000L;

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

        public static Func<ConsumeResult<byte[], byte[]>, ConsumeResult<byte[], byte[]>> ConverterForStore(
            IStateStore store)
            => WrappedStore.IsTimestamped(store) ? record => ToTimestampInstance(record) : record => record;
        
        
        private static ConsumeResult<byte[], byte[]> ToTimestampInstance(ConsumeResult<byte[], byte[]> source)
        {
            byte[] newValue = null;
            if (source.Message.Value != null)
            {
                using var buffer = ByteBuffer.Build(sizeof(long) + sizeof(int) + source.Message.Value.Length);
                newValue = buffer.PutLong(source.Message.Timestamp.UnixTimestampMs)
                    .PutInt(source.Message.Value.Length)
                    .Put(source.Message.Value)
                    .ToArray();
            }
            
            return new ConsumeResult<byte[], byte[]>
            {
                IsPartitionEOF = source.IsPartitionEOF,
                Message = new Message<byte[], byte[]>
                {
                    Headers = source.Message.Headers,
                    Timestamp = source.Message.Timestamp,
                    Key = source.Message.Key,
                    Value = newValue
                },
                Offset = source.Offset,
                TopicPartitionOffset = source.TopicPartitionOffset,
                Topic = source.Topic,
                Partition = source.Partition
            };
        }

    }
}
