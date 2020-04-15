using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Kafka
{
    public interface IConsumerRebalanceListener
    {
        void PartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions);
        void PartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions);
    }
}
