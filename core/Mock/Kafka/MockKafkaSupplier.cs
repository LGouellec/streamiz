using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockKafkaSupplier : IKafkaSupplier
    {
        private readonly MockCluster cluster = null;

        public MockKafkaSupplier(int defaultNumberPartitions = 1)
        {
            cluster = new MockCluster(defaultNumberPartitions);
        }

        public IAdminClient GetAdmin(AdminClientConfig config)
        {
            return new MockAdminClient(cluster);
        }

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            var consumer = new MockConsumer(cluster, config.GroupId, config.ClientId);
            consumer.SetRebalanceListener(rebalanceListener);
            return consumer;
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            return new MockProducer(cluster, config.ClientId);
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
            => GetConsumer(config, null);
    
        public void Destroy()
        {
            cluster.Destroy();
        }
    }
}
