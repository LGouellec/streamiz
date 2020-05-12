using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockWrappedConsumerRebalanceListener : IConsumerRebalanceListener
    {
        private readonly IConsumerRebalanceListener wrapped;
        private readonly MockConsumer consumer;

        public MockWrappedConsumerRebalanceListener(IConsumerRebalanceListener wrapped, MockConsumer consumer)
        {
            this.wrapped = wrapped;
            this.consumer = consumer;
        }

        public void PartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions)
        {
            this.consumer.PartitionsAssigned(partitions);
            wrapped?.PartitionsAssigned(consumer, partitions);
        }

        public void PartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions)
        {
            this.consumer.PartitionsRevoked(partitions);
            wrapped?.PartitionsRevoked(consumer, partitions);
        }
    }
}
