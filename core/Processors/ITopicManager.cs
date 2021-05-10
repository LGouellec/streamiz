using Confluent.Kafka;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    public interface ITopicManager
    {
        IAdminClient AdminClient { get; }

        IEnumerable<string> Apply(IDictionary<string, InternalTopicConfig> topics);
    }
}
