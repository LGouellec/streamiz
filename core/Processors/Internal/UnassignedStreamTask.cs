using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class UnassignedStreamTask : StreamTask
    {
        private readonly IStreamConfig config;

        internal class UnassignedProcessorContext : ProcessorContext
        {
            internal UnassignedProcessorContext(AbstractTask task, IStreamConfig configuration)
                : base(task, configuration, null, null)
            {
                FollowMetadata = false;
            }

            public override void Commit() { }

            internal override IStateStore GetStateStore(string storeName) => null;
        }

        UnassignedStreamTask(string threadId, TaskId id, IEnumerable<TopicPartition> partitions, ProcessorTopology processorTopology, IConsumer<byte[], byte[]> consumer, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, IProducer<byte[], byte[]> producer)
            : base(threadId, id, partitions, processorTopology, consumer, configuration, kafkaSupplier, producer, null, new StreamMetricsRegistry())
        {
            config = configuration;
        }

        public virtual new ProcessorContext Context => new UnassignedProcessorContext(this, config);

        public static UnassignedStreamTask Create()
        {
            var config = new StreamConfig();
            config.ApplicationId = "un-assigned-stream-task";
            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            return new UnassignedStreamTask(
                "un-assigned-stream-task",
                new TaskId { Id = -1, Partition = -1 },
                new List<TopicPartition>(),
                ProcessorTopology.EMPTY,
                consumer,
                config,
                supplier,
                producer);
        }
        
        public static UnassignedStreamTask Create(TaskId id)
        {
            var config = new StreamConfig();
            config.ApplicationId = "un-assigned-stream-task";
            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            return new UnassignedStreamTask(
                "un-assigned-stream-task",
                id,
                new List<TopicPartition>(),
                ProcessorTopology.EMPTY,
                consumer,
                config,
                supplier,
                producer);
        }

    }
}
