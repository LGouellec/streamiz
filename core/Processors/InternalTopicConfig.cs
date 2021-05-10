using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    public abstract class InternalTopicConfig
    {
        public static readonly string RETENTION_MS_CONFIG = "retention.ms";

        public string Name { get; set; }
        public IDictionary<string, string> Configs { get; set; }

        public abstract IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs);
    }
}
