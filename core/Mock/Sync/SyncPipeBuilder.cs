using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncPipeBuilder : IPipeBuilder
    {
        private readonly StreamTask task;
        private IKafkaSupplier kafkaSupplier;

        public SyncPipeBuilder(StreamTask task, IKafkaSupplier kafkaSupplier)
        {
            this.kafkaSupplier = kafkaSupplier;
            this.task = task;
        }

        public IPipeInput Input(string topic, IStreamConfig configuration)
        {
            return new SyncPipeInput(task);
        }

        public IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default)
        {
            return new SyncPipeOutput(topic, consumeTimeout, configuration, kafkaSupplier, token);
        }
    }
}