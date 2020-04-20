using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockKafkaSupplier : IKafkaSupplier
    {
        public IAdminClient GetAdmin(AdminClientConfig config)
        {
            return new MockAdminClient();
        }

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            var consumer = new MockConsumer(config.GroupId, config.ClientId);
            consumer.SetRebalanceListener(rebalanceListener);
            return consumer;
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            return new MockProducer(config.ClientId);
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
            => GetConsumer(config, null);
    }
}
