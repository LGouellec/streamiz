using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace Kafka.Streams.Net.Processors.Internal
{
    internal class RecordContext : IRecordContext
    {
        public RecordContext(ConsumeResult<byte[], byte[]> result)
        {
            this.Offset = result.Offset;
            this.Timestamp = result.Message.Timestamp.UnixTimestampMs;
            this.Topic = result.Topic;
            this.Partition = result.Partition;
            this.Headers = result.Message.Headers;
        }

        public long Offset { get; }

        public long Timestamp { get; }

        public string Topic { get; }

        public int Partition { get; }

        public Headers Headers { get; }
    }
}
