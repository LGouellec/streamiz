using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    public class DefaultTopicManager : ITopicManager
    {
        public DefaultTopicManager(
            IStreamConfig config,
            IAdminClient adminClient)
        {

        }

        public IAdminClient AdminClient { get; private set; }

        public IEnumerable<string> Apply(IDictionary<string, InternalTopicConfig> topics)
        {
            throw new NotImplementedException();
        }
    }
}
