using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    public class WindowedChangelogTopicConfig : InternalTopicConfig
    {
        private long retentionsMs = 0;

        public long RetentionMs
        {
            set
            {
                if (!Configs.ContainsKey(RETENTION_MS_CONFIG))
                    retentionsMs = value;
            }
        }

        public override IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs)
        {
            throw new NotImplementedException();
        }
    }
}
