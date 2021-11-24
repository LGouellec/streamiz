using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TaskCreator : AbstractTaskCreator<StreamTask>
    {
        private readonly InternalTopologyBuilder builder;
        private readonly IStreamConfig configuration;
        private readonly string threadId;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly IProducer<byte[], byte[]> producer;
        private readonly StoreChangelogReader storeChangelogReader;

        public TaskCreator(InternalTopologyBuilder builder, IStreamConfig configuration, string threadId, IKafkaSupplier kafkaSupplier, IProducer<byte[], byte[]> producer, StoreChangelogReader storeChangelogReader)
            : base()
        {
            this.builder = builder;
            this.configuration = configuration;
            this.threadId = threadId;
            this.kafkaSupplier = kafkaSupplier;
            this.producer = producer;
            this.storeChangelogReader = storeChangelogReader;
        }

        public override StreamTask CreateTask(IConsumer<byte[], byte[]> consumer, TaskId id, IEnumerable<TopicPartition> partitions)
        {
            log.Debug($"Created task {id} with assigned partition {string.Join(",", partitions)}");
            return new StreamTask(
                threadId,
                id,
                partitions,
                builder.BuildTopology(id.Id),
                consumer,
                configuration,
                kafkaSupplier,
                producer,
                storeChangelogReader);
        }
    }
}