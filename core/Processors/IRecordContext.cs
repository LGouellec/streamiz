using Confluent.Kafka;
using System;

namespace kafka_stream_core.Processors
{
    public interface IRecordContext
    {
        long Offset { get; }
        long Timestamp { get; }
        string Topic { get; }
        int Partition { get; }
        Headers Headers { get; }
    }
}
