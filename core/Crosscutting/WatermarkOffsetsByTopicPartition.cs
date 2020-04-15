using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Crosscutting
{
    internal class WatermarkOffsetsByTopicPartition : WatermarkOffsets
    {
        public string Topic { get; }
        public int Partition { get; }

        public WatermarkOffsetsByTopicPartition(string topic, int partition, Offset low, Offset high) 
            : base(low, high)
        {
            Topic = topic;
            Partition = partition;
        }
    }
}
