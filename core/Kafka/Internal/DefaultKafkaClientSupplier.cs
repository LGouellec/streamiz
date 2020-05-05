using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class DefaultKafkaClientSupplier : IKafkaSupplier
    {
        private readonly KafkaLoggerAdapter loggerAdapter = null;

        public DefaultKafkaClientSupplier(KafkaLoggerAdapter loggerAdapter)
        {
            if (loggerAdapter == null)
                throw new ArgumentNullException(nameof(loggerAdapter));

            this.loggerAdapter = loggerAdapter;
        }

        public IAdminClient GetAdmin(AdminClientConfig config)
        {
            AdminClientBuilder builder = new AdminClientBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogAdmin);
            builder.SetErrorHandler(loggerAdapter.ErrorAdmin);
            return builder.Build();
        }

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            ConsumerBuilder<byte[], byte[]> builder = new ConsumerBuilder<byte[], byte[]>(config);
            if (rebalanceListener != null)
            {
                builder.SetPartitionsAssignedHandler((c, p) => rebalanceListener.PartitionsAssigned(c, p));
                builder.SetPartitionsRevokedHandler((c, p) => rebalanceListener.PartitionsRevoked(c, p));
                builder.SetLogHandler(loggerAdapter.LogConsume);
                builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            }
            return builder.Build();
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            ProducerBuilder<byte[], byte[]> builder = new ProducerBuilder<byte[], byte[]>(config);
            builder.SetLogHandler(loggerAdapter.LogProduce);
            builder.SetErrorHandler(loggerAdapter.ErrorProduce);
            return builder.Build();
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
        {
            ConsumerBuilder<byte[], byte[]> builder = new ConsumerBuilder<byte[], byte[]>(config);
            // TOOD : Finish
            builder.SetLogHandler(loggerAdapter.LogConsume);
            builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            return builder.Build();
        }
    }
}