using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Mock.Pipes
{
    internal class PipeBuilder
    {
        internal IPipeInput Input(string topic, ProducerConfig configuration)
        {
            return null;
        }

        internal IPipeOutput Output(string topic, TimeSpan consumeTimeout, ConsumerConfig configuration)
        {
            return null;
        }
    }
}
