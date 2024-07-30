using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Mock
{
    internal class MockChangelogRegister : IChangelogRegister
    {
        private readonly IList<TopicPartition> restoringPartitions = new List<TopicPartition>();

        public MockChangelogRegister(){ }

        public void Register(TopicPartition topicPartition, ProcessorStateManager processorStateManager)
        {
            restoringPartitions.Add(topicPartition);
        }

        public void Unregister(IEnumerable<TopicPartition> topicPartitions)
        {
            foreach (var tp in topicPartitions)
                restoringPartitions.Remove(tp);
        }
    }
}
