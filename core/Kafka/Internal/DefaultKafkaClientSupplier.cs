using Confluent.Kafka;
using System;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Librdkafka;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class DefaultKafkaClientSupplier : IKafkaSupplier
    {
        private readonly KafkaLoggerAdapter loggerAdapter = null;
        private readonly IStreamConfig streamConfig;
        private readonly bool exposeLibrdKafka;

        public DefaultKafkaClientSupplier(KafkaLoggerAdapter loggerAdapter)
            : this(loggerAdapter, null)
        { }

        
        public DefaultKafkaClientSupplier(
            KafkaLoggerAdapter loggerAdapter,
            IStreamConfig streamConfig)
        {
            if (loggerAdapter == null)
                throw new ArgumentNullException(nameof(loggerAdapter));

            this.loggerAdapter = loggerAdapter;
            this.streamConfig = streamConfig;
            exposeLibrdKafka = streamConfig?.ExposeLibrdKafkaStats ?? false;
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
                if (exposeLibrdKafka)
                {
                    var consumerStatisticsHandler = new ConsumerStatisticsHandler(
                        config.ClientId,
                        streamConfig.ApplicationId);
                    consumerStatisticsHandler.Register(MetricsRegistry);
                    builder.SetStatisticsHandler((c, stat) =>
                    {
                        var statistics = JsonConvert.DeserializeObject<Statistics>(stat);
                        consumerStatisticsHandler.Publish(statistics);
                    });
                }
            }
            return builder.Build();
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            ProducerBuilder<byte[], byte[]> builder = new ProducerBuilder<byte[], byte[]>(config);
            builder.SetLogHandler(loggerAdapter.LogProduce);
            builder.SetErrorHandler(loggerAdapter.ErrorProduce);
            if (exposeLibrdKafka)
            {
                var producerStatisticsHandler = new ProducerStatisticsHandler(
                    streamConfig.ClientId,
                    streamConfig.ApplicationId);
                producerStatisticsHandler.Register(MetricsRegistry);
                builder.SetStatisticsHandler((c, stat) =>
                {
                    var statistics = JsonConvert.DeserializeObject<Statistics>(stat);
                    producerStatisticsHandler.Publish(statistics);
                });
            }
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

        public IConsumer<byte[], byte[]> GetGlobalConsumer(ConsumerConfig config)
        {
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            ConsumerBuilder<byte[], byte[]> builder = new ConsumerBuilder<byte[], byte[]>(config);
            // TOOD : Finish
            builder.SetLogHandler(loggerAdapter.LogConsume);
            builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            return builder.Build();
        }
        
        public StreamMetricsRegistry MetricsRegistry { get; set; }
    }
}