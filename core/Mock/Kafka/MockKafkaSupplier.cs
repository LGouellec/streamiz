using Confluent.Kafka;
using kafka_stream_core.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Mock.Kafka
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
            if(rebalanceListener != null)
                consumer.SetRebalanceListener(rebalanceListener);
            return consumer;
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            return new MockProducer();
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
            => GetConsumer(config, null);
    }
}
