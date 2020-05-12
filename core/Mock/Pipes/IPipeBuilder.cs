using System;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal interface IPipeBuilder
    {
        IPipeInput Input(string topic, IStreamConfig configuration);

        IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default);
    }
}
