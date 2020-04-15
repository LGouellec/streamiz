using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Kafka
{
    public interface IKafkaSupplier
    {
        IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener);
        IProducer<byte[], byte[]> GetProducer(ProducerConfig config);
        IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config);
        IAdminClient GetAdmin(AdminClientConfig config);
    }
}
