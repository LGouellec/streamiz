using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    /// <summary>
    /// This supplier provides a synchronous kafka cluster in memory.
    /// Used for synchronous unit test.
    /// He support EOS Feature.
    /// </summary>
    internal class SyncKafkaSupplier : IKafkaSupplier
    {
        private SyncProducer producer = null;

        public IAdminClient GetAdmin(AdminClientConfig config) => new SyncAdminClient();

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            var consumer = new SyncConsumer(config, producer);
            consumer.SetRebalanceListener(rebalanceListener);
            return consumer;
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            if (producer == null)
                producer = new SyncProducer(config);
            return producer;
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
            => GetConsumer(config, null);
    }
}
