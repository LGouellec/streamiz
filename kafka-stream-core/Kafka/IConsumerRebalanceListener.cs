using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Kafka
{
    public interface IConsumerRebalanceListener
    {
        void PartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions);
        void PartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions);
    }
}
