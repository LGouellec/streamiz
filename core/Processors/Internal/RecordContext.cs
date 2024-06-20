using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class RecordContext : IRecordContext
    {
        public RecordContext()
            : this(new Headers(), -1, -1, -1, "")
        {
        }

        public RecordContext(Headers headers, long offset, long timestamp, int partition, string topic)
        {
            Offset = offset;
            Timestamp = timestamp;
            Topic = topic;
            Partition = partition;
            Headers = headers;
        }
        
        public RecordContext(ConsumeResult<byte[], byte[]> result)
        : this(result.Message.Headers, result.Offset, result.Message.Timestamp.UnixTimestampMs, result.Partition, result.Topic)
        {
        }

        public long Offset { get; }

        public long Timestamp { get; private set; }

        public string Topic { get; }

        public int Partition { get; }

        public Headers Headers { get; internal set; }

        public void ChangeTimestamp(long ts)
        {
            Timestamp = ts;
        }

        public void SetHeaders(Headers headers)
        {
            Headers = headers;
        }
    }
}
