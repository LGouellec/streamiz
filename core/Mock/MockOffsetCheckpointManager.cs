using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Mock
{
    internal class MockOffsetCheckpointManager :  IOffsetCheckpointManager
    {
        public MockOffsetCheckpointManager()
        {
        }

        public void Configure(IStreamConfig config, TaskId taskId)
        {
        }

        public void Destroy(TaskId taskId)
        {
        }

        public IDictionary<TopicPartition, long> Read(TaskId taskId)
            => new Dictionary<TopicPartition, long>();

        public void Write(TaskId taskId, IDictionary<TopicPartition, long> data)
        {
        }
    }
}
