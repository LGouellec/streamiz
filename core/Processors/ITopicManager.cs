using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface ITopicManager : IDisposable
    {
        IAdminClient AdminClient { get; }

        Task<IEnumerable<string>> ApplyAsync(int topologyId, IDictionary<string, InternalTopicConfig> topics);
    }
}
