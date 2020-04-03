using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace kafka_stream_core.Processors.Internal
{
    internal class RecordContext : IRecordContext
    {
        private readonly ConsumeResult<byte[], byte[]> result;

        public RecordContext(ConsumeResult<byte[], byte[]> result)
        {
            this.Offset = result.Offset;
            this.Timestamp = result.Timestamp.UnixTimestampMs;
            this.Topic = result.Topic;
            this.Partition = result.Partition;
            this.Headers = result.Headers;
        }

        public long Offset { get; }

        public long Timestamp { get; }

        public string Topic { get; }

        public int Partition { get; }

        public Headers Headers { get; }
    }
}
