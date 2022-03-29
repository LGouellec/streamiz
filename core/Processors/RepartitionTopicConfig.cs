using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors
{
    internal class RepartitionTopicConfig : InternalTopicConfig
    {
        public static IDictionary<string, string> REPARTITION_TOPIC_DEFAULT_OVERRIDES = new Dictionary<string, string>(INTERNAL_TOPIC_DEFAULT_OVERRIDES)
        {
            { InternalTopicConfigCst.CLEANUP_POLICY_CONFIG, "delete"},
            { InternalTopicConfigCst.SEGMENT_BYTES_CONFIG, "52428800"}, // 50MB
            // TODO : set infinity, and purge records automatically
            { InternalTopicConfigCst.RETENTION_MS_CONFIG, "-1"}, // Infinity
        };
        
        public override IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs)
        {
            IDictionary<string, string> topicConfig = new Dictionary<string, string>(REPARTITION_TOPIC_DEFAULT_OVERRIDES);
            topicConfig.AddRange(defaultConfigs);
            topicConfig.AddRange(Configs);
            return topicConfig;
        }
    }
}