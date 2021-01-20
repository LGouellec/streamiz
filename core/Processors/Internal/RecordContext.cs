using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class RecordContext : IRecordContext
    {
        public RecordContext(ConsumeResult<byte[], byte[]> result)
        {
            Offset = result.Offset;
            Timestamp = result.Message.Timestamp.UnixTimestampMs;
            Topic = result.Topic;
            Partition = result.Partition;
            Headers = result.Message.Headers;
        }

        public long Offset { get; }

        public long Timestamp { get; private set; }

        public string Topic { get; }

        public int Partition { get; }

        public Headers Headers { get; }

        public void ChangeTimestamp(long ts)
        {
            Timestamp = ts;
        }
    }
}
