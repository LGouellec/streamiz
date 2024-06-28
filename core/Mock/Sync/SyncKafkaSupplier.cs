using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    /// <summary>
    /// This supplier provides a synchronous kafka cluster in memory.
    /// Used for synchronous unit test.
    /// He support EOS Feature.
    /// </summary>
    internal class SyncKafkaSupplier : IKafkaSupplier
    {
        protected SyncProducer producer = null;
        protected SyncAdminClient admin = null;

        public SyncKafkaSupplier(bool autoCreateTopic = true)
        {
            producer = new SyncProducer();
            admin = new SyncAdminClient(producer, autoCreateTopic);   
        }

        public virtual IAdminClient GetAdmin(AdminClientConfig config)
        {
            admin.UseConfig(config);
            return admin;
        }

        public virtual IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            var consumer = new SyncConsumer(producer);
            consumer.UseConfig(config);
            consumer.SetRebalanceListener(rebalanceListener);
            return consumer;
        }

        public virtual IConsumer<byte[], byte[]> GetGlobalConsumer(ConsumerConfig config)
        {
            var globalConsumer = new SyncConsumer(producer);
            globalConsumer.UseConfig(config);
            globalConsumer.SetRebalanceListener(null);
            return globalConsumer;
        }

        public StreamMetricsRegistry MetricsRegistry { get; set; }

        public virtual IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            producer.UseConfig(config);
            return producer;
        }

        public virtual IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
        {
            var restoreConsumer = new SyncConsumer(producer);
            restoreConsumer.UseConfig(config);
            restoreConsumer.SetRebalanceListener(null);
            return restoreConsumer;
        }
    }
}
