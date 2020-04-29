using Streamiz.Kafka.Net.Kafka;
using System;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class SyncPipeOutput : KafkaPipeOutput
    {
        public SyncPipeOutput(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, CancellationToken token)
            : base(topic, consumeTimeout, configuration, kafkaSupplier, token)
        {
        }
    }
}
