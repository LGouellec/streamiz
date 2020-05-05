using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    /// <summary>
    /// This supplier provides a asynchronous kafka cluster in memory.
    /// Used for asynchronous unit test.
    /// Warning the cluster is encapsulated by <see cref="MockKafkaSupplier" /> instance.
    /// For moment, he doesn't support EOS Feature. If you need that, please look to <see cref="Sync.SyncKafkaSupplier"/>
    /// </summary>
    internal class MockKafkaSupplier : IKafkaSupplier
    {
        private readonly MockCluster cluster = null;

        public MockKafkaSupplier(int defaultNumberPartitions = 1)
        {
            cluster = new MockCluster(defaultNumberPartitions);
        }

        public IAdminClient GetAdmin(AdminClientConfig config)
        {
            return new MockAdminClient(cluster, config.ClientId);
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
