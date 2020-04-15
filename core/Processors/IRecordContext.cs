using Confluent.Kafka;
using System;

namespace Kafka.Streams.Net.Processors
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
