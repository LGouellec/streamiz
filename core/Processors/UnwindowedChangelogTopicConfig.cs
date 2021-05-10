using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    public class UnwindowedChangelogTopicConfig : InternalTopicConfig
    {
        public override IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs)
        {
            throw new NotImplementedException();
        }
    }
}
