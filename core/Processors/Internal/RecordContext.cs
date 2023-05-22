using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class RecordContext : IRecordContext
    {
        public RecordContext()
        {
            Offset = -1;
            Timestamp = -1;
            Topic = "";
            Partition = -1;
            Headers = new Headers();
        }
        
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
