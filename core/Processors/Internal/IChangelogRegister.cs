using System.Collections.Generic;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IChangelogRegister
    {
        void Register(TopicPartition topicPartition, ProcessorStateManager processorStateManager);
        void Unregister(IEnumerable<TopicPartition> topicPartitions);
    }
}
