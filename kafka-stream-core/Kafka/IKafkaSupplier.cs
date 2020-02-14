using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Kafka
{
    public interface IKafkaSupplier
    {
        IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config);
        IProducer<byte[], byte[]> GetProducer(ProducerConfig config);
        IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config);
        IAdminClient GetAdmin(AdminClientConfig config);
    }
}
