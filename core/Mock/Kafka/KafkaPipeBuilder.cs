using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using System;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class KafkaPipeBuilder : IPipeBuilder
    {
        private IKafkaSupplier kafkaSupplier;

        public KafkaPipeBuilder(IKafkaSupplier kafkaSupplier)
        {
            this.kafkaSupplier = kafkaSupplier;
        }

        public IPipeInput Input(string topic, IStreamConfig configuration)
        {
            return new KafkaPipeInput(topic, configuration, kafkaSupplier);
        }

        public IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default)
        {
            return new KafkaPipeOutput(topic, consumeTimeout, configuration, kafkaSupplier, token);
        }
    }
}
