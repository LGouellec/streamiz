using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.State
{
    public interface IOffsetCheckpointManager
    {
        IDictionary<TopicPartition, long> Read(TaskId taskId);
        void Configure(IStreamConfig config, TaskId taskId);
        void Write(TaskId taskId, IDictionary<TopicPartition, long> data);
        void Destroy(TaskId taskId);
    }
}