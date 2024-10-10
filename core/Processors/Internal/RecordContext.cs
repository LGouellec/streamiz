using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;

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

        public long MemorySizeEstimate
            => sizeof(int) + // partition
               sizeof(long) + //offset
               sizeof(long) + // timestamp
               Topic.Length + // topic length
               Headers.GetEstimatedSize(); // headers size

        public byte[] Serialize()
        {
            var size = sizeof(int) + // partition
                       sizeof(long) + //offset
                       sizeof(long) + // timestamp 
                       sizeof(int) + // topic length
                       Topic.Length + // topic
                       sizeof(int); // number of headers
            
            List<(byte[], byte[])> byteHeaders = new();
            
            foreach(var header in Headers)
            {
                size += 2 * sizeof(int); // size of key and value
                size += header.Key.Length;
                if (header.GetValueBytes() != null)
                    size += header.GetValueBytes().Length;
                byteHeaders.Add((Encoding.UTF8.GetBytes(header.Key), header.GetValueBytes()));
            }

            var buffer = ByteBuffer.Build(size, true);
            buffer.PutInt(Partition)
                .PutLong(Offset)
                .PutLong(Timestamp)
                .PutInt(Topic.Length)
                .Put(Encoding.UTF8.GetBytes(Topic))
                .PutInt(byteHeaders.Count);

            foreach (var byteHeader in byteHeaders)
            {
                buffer.PutInt(byteHeader.Item1.Length).Put(byteHeader.Item1);
                if (byteHeader.Item2 != null)
                    buffer.PutInt(byteHeader.Item2.Length).Put(byteHeader.Item2);
                else
                    buffer.PutInt(-1);
            }

            return buffer.ToArray();
        }

        internal static IRecordContext Deserialize(ByteBuffer buffer)
        {
            var partition = buffer.GetInt();
            var offset = buffer.GetLong();
            var timestamp = buffer.GetLong();
            var topic = Encoding.UTF8.GetString(buffer.GetBytes(buffer.GetInt()));
            var headerCount = buffer.GetInt();
            Headers headers = new Headers();
            
            for (int i = 0; i < headerCount; ++i)
            {
                var headerKeyLength = buffer.GetInt();
                var headerKey = buffer.GetBytes(headerKeyLength);
                byte[] headerValue = null;
                var headerValueLength = buffer.GetInt();

                if (headerValueLength > 0)
                    headerValue = buffer.GetBytes(headerValueLength);
                
                headers.Add(Encoding.UTF8.GetString(headerKey), headerValue);
            }
            
            return new RecordContext(headers, offset, timestamp, partition, topic);
        } 
    }
}
