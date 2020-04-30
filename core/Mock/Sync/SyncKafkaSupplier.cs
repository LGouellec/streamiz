using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncKafkaSupplier : IKafkaSupplier
    {
        private readonly IList<SyncProducer> producers = new List<SyncProducer>();

        public IAdminClient GetAdmin(AdminClientConfig config) => new SyncAdminClient();

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
            => new SyncConsumer(config, this);

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            var producer = new SyncProducer(config);
            producers.Add(producer);
            return producer;
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
            => GetConsumer(config, null);

        internal SyncProducer GetProducerInstance(string topicName) =>
            producers.FirstOrDefault(p => p.IsMatch(topicName));
    }
}
