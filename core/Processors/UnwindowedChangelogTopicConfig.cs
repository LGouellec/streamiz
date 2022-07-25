using Streamiz.Kafka.Net.Crosscutting;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal class UnwindowedChangelogTopicConfig : InternalTopicConfig
    {
        public static IDictionary<string, string> UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES = new Dictionary<string, string>(INTERNAL_TOPIC_DEFAULT_OVERRIDES)
        {
            { InternalTopicConfigCst.CLEANUP_POLICY_CONFIG, "compact"}
        };

        public override IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs)
        {
            IDictionary<string, string> topicConfig = new Dictionary<string, string>(UNWINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);
            topicConfig.AddRange(defaultConfigs);
            topicConfig.AddRange(Configs);
            return topicConfig;
        }
    }
}
