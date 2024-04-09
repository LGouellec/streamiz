using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.State.Cache
{
    public class CacheEntryValue
    {
        public byte[] Value { get; }
        public IRecordContext Context { get; }

        public CacheEntryValue(byte[] value)
        {
            Context = new RecordContext();
            Value = value;
        }

        public CacheEntryValue(byte[] value, Headers headers, long offset, long timestamp, int partition, string topic)
        {
            Context = new RecordContext(headers.Clone(), offset, timestamp, partition, topic);
            Value = value;
        }

        public long Size =>
            (Value != null ? Value.LongLength : 0) +
            sizeof(int) + // partition
            sizeof(long) + //offset
            sizeof(long) + // timestamp
            Context.Topic.Length + // topic length
            Context.Headers.GetEstimatedSize(); // headers size
    }
}