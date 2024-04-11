using System;
using Confluent.Kafka;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Librdkafka;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class DefaultKafkaClientBuilder
    {
        public virtual ConsumerBuilder<byte[], byte[]> GetConsumerBuilder(ConsumerConfig config)
            => new(config);
        
        public virtual AdminClientBuilder GetAdminBuilder(AdminClientConfig config)
            => new(config);

        public virtual ProducerBuilder<byte[], byte[]> GetProducerBuilder(ProducerConfig config)
            => new(config);
    }
    
    internal class DefaultKafkaClientSupplier : IKafkaSupplier
    {
        private readonly KafkaLoggerAdapter loggerAdapter;
        private readonly IStreamConfig streamConfig;
        private readonly bool exposeLibrdKafka;
        private readonly DefaultKafkaClientBuilder builderKafkaHandler;

        public DefaultKafkaClientSupplier(KafkaLoggerAdapter loggerAdapter)
            : this(loggerAdapter, null)
        { }

        public DefaultKafkaClientSupplier(
            KafkaLoggerAdapter loggerAdapter,
            IStreamConfig streamConfig)
            : this(loggerAdapter, streamConfig, new DefaultKafkaClientBuilder())
        { }

        internal DefaultKafkaClientSupplier(
            KafkaLoggerAdapter loggerAdapter,
            IStreamConfig streamConfig,
            DefaultKafkaClientBuilder builderKafkaHandler)
        {
            if (loggerAdapter == null)
                throw new ArgumentNullException(nameof(loggerAdapter));

            this.loggerAdapter = loggerAdapter;
            this.streamConfig = streamConfig;
            exposeLibrdKafka = streamConfig?.ExposeLibrdKafkaStats ?? false;
            this.builderKafkaHandler = builderKafkaHandler ?? new DefaultKafkaClientBuilder();
        }

        public IAdminClient GetAdmin(AdminClientConfig config)
        {
            AdminClientBuilder builder = builderKafkaHandler.GetAdminBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogAdmin);
            builder.SetErrorHandler(loggerAdapter.ErrorAdmin);
            return builder.Build();
        }

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            ConsumerBuilder<byte[], byte[]> builder = builderKafkaHandler.GetConsumerBuilder(config);
            if (rebalanceListener != null)
            {
                builder.SetPartitionsAssignedHandler(rebalanceListener.PartitionsAssigned);
                builder.SetPartitionsRevokedHandler(rebalanceListener.PartitionsRevoked);
                builder.SetLogHandler(loggerAdapter.LogConsume);
                builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            }
            
            if (exposeLibrdKafka)
            {
                // TODO : test librdkafka statistics with IntegrationTest (WIP see #82)
                var consumerStatisticsHandler = new ConsumerStatisticsHandler(
                    config.ClientId,
                    config is StreamizConsumerConfig streamizConsumerConfig  && streamizConsumerConfig.Config != null ?
                        streamizConsumerConfig.Config.ApplicationId :
                        streamConfig.ApplicationId, 
                    (config as StreamizConsumerConfig)?.ThreadId);
                consumerStatisticsHandler.Register(MetricsRegistry);
                builder.SetStatisticsHandler((c, stat) =>
                {
                    var statistics = JsonConvert.DeserializeObject<Statistics>(stat);
                    consumerStatisticsHandler.Publish(statistics);
                });
            }

            return builder.Build();
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            ProducerBuilder<byte[], byte[]> builder = builderKafkaHandler.GetProducerBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogProduce);
            builder.SetErrorHandler(loggerAdapter.ErrorProduce);
            if (exposeLibrdKafka)
            {
                // TODO : test librdkafka statistics with IntegrationTest (WIP see #82)
                var producerStatisticsHandler = new ProducerStatisticsHandler(
                    config.ClientId,
                    config is StreamizProducerConfig streamizProducerConfig  && streamizProducerConfig.Config != null ?
                        streamizProducerConfig.Config.ApplicationId :
                        streamConfig.ApplicationId,
                    (config as StreamizProducerConfig)?.ThreadId,
                    (config as StreamizProducerConfig)?.Id?.ToString());
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
            ConsumerBuilder<byte[], byte[]> builder = builderKafkaHandler.GetConsumerBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogConsume);
            builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            return builder.Build();
        }

        public IConsumer<byte[], byte[]> GetGlobalConsumer(ConsumerConfig config)
        {
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            ConsumerBuilder<byte[], byte[]> builder = builderKafkaHandler.GetConsumerBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogConsume);
            builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            
            if (exposeLibrdKafka)
            {
                var consumerStatisticsHandler = new ConsumerStatisticsHandler(
                    config.ClientId,
                    config is StreamizConsumerConfig streamizConsumerConfig  && streamizConsumerConfig.Config != null ?
                        streamizConsumerConfig.Config.ApplicationId :
                        streamConfig.ApplicationId, 
                    (config as StreamizConsumerConfig)?.ThreadId, 
                    true);
                consumerStatisticsHandler.Register(MetricsRegistry);
                builder.SetStatisticsHandler((c, stat) =>
                {
                    var statistics = JsonConvert.DeserializeObject<Statistics>(stat);
                    consumerStatisticsHandler.Publish(statistics);
                });
            }
            
            return builder.Build();
        }
        
        public StreamMetricsRegistry MetricsRegistry { get; set; }
    }
}