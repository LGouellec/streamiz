using Confluent.Kafka;
using System;

namespace kafka_stream_core.Processors
{
    public interface RecordContext
    {
        long offset { get; }
        long timestamp { get; }
        String topic { get; }
        int partition { get; }
        Headers headers { get; }
    }
}
