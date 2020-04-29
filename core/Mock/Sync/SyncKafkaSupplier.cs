using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncKafkaSupplier : IKafkaSupplier
    {
        public IAdminClient GetAdmin(AdminClientConfig config) => new SyncAdminClient();

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
            => new SyncConsumer(config);

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
            => new SyncProducer(config);

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
            => GetConsumer(config, null);
    }
}
