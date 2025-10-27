using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Metrics;

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
        private readonly bool _isDestroyable;
        private readonly MockCluster cluster = null;

        public MockKafkaSupplier(int defaultNumberPartitions = 1, long waitBeforeRebalanceMs = 0, bool isDestroyable = true)
        {
            _isDestroyable = isDestroyable;
            cluster = new MockCluster(defaultNumberPartitions, waitBeforeRebalanceMs);
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

        public IConsumer<byte[], byte[]> GetGlobalConsumer(ConsumerConfig config)
            => GetConsumer(config, null);

        public StreamMetricsRegistry MetricsRegistry { get; set; }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            return new MockProducer(cluster, config.ClientId);
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
            => GetConsumer(config, null);
    
        public void Destroy()
        {
            if(_isDestroyable)
                cluster.Destroy();
        }

        public void CreateTopic(string topic)
            => cluster.CreateTopic(topic);
    }
}
