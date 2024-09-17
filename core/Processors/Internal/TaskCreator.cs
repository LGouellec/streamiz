using System.Collections.Generic;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;

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
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly Sensor createTaskSensor;

        internal IStreamConfig Configuration => configuration;
        
        public TaskCreator(InternalTopologyBuilder builder, IStreamConfig configuration, string threadId,
            IKafkaSupplier kafkaSupplier, StoreChangelogReader storeChangelogReader,
            StreamMetricsRegistry streamMetricsRegistry)
        {
            this.builder = builder;
            this.configuration = configuration;
            this.threadId = threadId;
            this.kafkaSupplier = kafkaSupplier;
            this.storeChangelogReader = storeChangelogReader;
            this.streamMetricsRegistry = streamMetricsRegistry;

            createTaskSensor = ThreadMetrics.CreateTaskSensor(threadId, streamMetricsRegistry);
        }

        public override StreamTask CreateTask(IConsumer<byte[], byte[]> consumer,  StreamsProducer producer, TaskId id, IEnumerable<TopicPartition> partitions)
        {
            log.LogDebug($"Created task {id} with assigned partition {string.Join(",", partitions)}");
            var task = new StreamTask(
                threadId,
                id,
                partitions,
                builder.BuildTopology(id.Id),
                consumer,
                configuration,
                kafkaSupplier,
                producer,
                storeChangelogReader,
                streamMetricsRegistry);
            
            createTaskSensor.Record();
            
            return task;
        }
    }
}