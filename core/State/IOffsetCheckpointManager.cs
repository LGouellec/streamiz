using System.Collections.Generic;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.State
{
    public interface IOffsetCheckpointManager
    {
        IDictionary<TopicPartition, long> Read();
        void Write(IDictionary<TopicPartition, long> data);
        void Destroy();
    }
}