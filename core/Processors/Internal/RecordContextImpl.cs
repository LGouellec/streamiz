using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace kafka_stream_core.Processors.Internal
{
    internal class RecordContextImpl : RecordContext
    {
        private readonly ConsumeResult<byte[], byte[]> result;

        public RecordContextImpl(ConsumeResult<byte[], byte[]> result)
        {
            this.offset = result.Offset;
            this.timestamp = result.Timestamp.UnixTimestampMs;
            this.topic = result.Topic;
            this.partition = result.Partition;
            this.headers = result.Headers;
        }

        public long offset { get; }

        public long timestamp { get; }

        public string topic { get; }

        public int partition { get; }

        public Headers headers { get; }
    }
}
