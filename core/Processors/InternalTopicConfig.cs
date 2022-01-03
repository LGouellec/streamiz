using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    public static class InternalTopicConfigCst
    {
        public static readonly string RETENTION_MS_CONFIG = "retention.ms";
        public static readonly string MESSAGE_TIMESTAMP_TYPE_CONFIG = "message.timestamp.type";
        public static readonly string CLEANUP_POLICY_CONFIG = "cleanup.policy";
        public static readonly string SEGMENT_BYTES_CONFIG = "segment.bytes";
    }

    public abstract class InternalTopicConfig
    {
        public static IDictionary<string, string> INTERNAL_TOPIC_DEFAULT_OVERRIDES = new Dictionary<string, string>()
        {
            {InternalTopicConfigCst.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime" }
        };

        public string Name { get; set; }
        public IDictionary<string, string> Configs { get; set; } = new Dictionary<string, string>();
        internal int NumberPartitions { get; set; }

        public abstract IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs);
    }
}
