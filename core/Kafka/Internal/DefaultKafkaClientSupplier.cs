using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace kafka_stream_core.Kafka.Internal
{
    internal class DefaultKafkaClientSupplier : IKafkaSupplier
    {
        public IAdminClient GetAdmin(AdminClientConfig config)
        {
            AdminClientBuilder builder = new AdminClientBuilder(config);
            return builder.Build();
        }

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            ConsumerBuilder<byte[], byte[]> builder = new ConsumerBuilder<byte[], byte[]>(config);
            if (rebalanceListener != null)
            {
                builder.SetPartitionsAssignedHandler((c, p) => rebalanceListener.PartitionsAssigned(c, p));
                builder.SetPartitionsRevokedHandler((c, p) => rebalanceListener.PartitionsRevoked(c, p));
            }
            return builder.Build();
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            ProducerBuilder<byte[], byte[]> builder = new ProducerBuilder<byte[], byte[]>(config);
            return builder.Build();
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
        {
            ConsumerBuilder<byte[], byte[]> builder = new ConsumerBuilder<byte[], byte[]>(config);
            return builder.Build();
        }
    }
}
