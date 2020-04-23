using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using log4net;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TaskCreator : AbstractTaskCreator<StreamTask>
    {
        private InternalTopologyBuilder builder;
        private IStreamConfig configuration;
        private string threadId;
        private IKafkaSupplier kafkaSupplier;
        private IProducer<byte[], byte[]> producer;
        private readonly ILog log = Logger.GetLogger(typeof(TaskCreator));

        public TaskCreator(InternalTopologyBuilder builder, IStreamConfig configuration, string threadId, IKafkaSupplier kafkaSupplier, IProducer<byte[], byte[]> producer)
            : base(builder, configuration)
        {
            this.builder = builder;
            this.configuration = configuration;
            this.threadId = threadId;
            this.kafkaSupplier = kafkaSupplier;
            this.producer = producer;
        }

        public override StreamTask CreateTask(IConsumer<byte[], byte[]> consumer, TaskId id, TopicPartition partition)
        {
            var processorTopology = this.builder.BuildTopology();
            log.Debug($"Created task {id} with assigned partition {partition}");

            return new StreamTask(
                threadId,
                id,
                partition,
                processorTopology,
                consumer,
                configuration,
                kafkaSupplier,
                producer);
        }
    }
}
