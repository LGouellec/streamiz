using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace kafka_stream_core.Mock.Pipes
{
    internal class PipeBuilder
    {
        internal IPipeInput Input(string topic, IStreamConfig configuration)
        {
            return new PipeInput(topic, configuration);
        }

        internal IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default)
        {
            return new PipeOutput(topic, consumeTimeout, configuration, token);
        }
    }
}
