using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal interface IPipeBuilder
    {
        IPipeInput Input(string topic, IStreamConfig configuration);

        IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default);
    }
}
