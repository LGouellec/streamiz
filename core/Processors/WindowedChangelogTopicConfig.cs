using Streamiz.Kafka.Net.Crosscutting;
using System.Collections.Generic;


namespace Streamiz.Kafka.Net.Processors
{
    internal class WindowedChangelogTopicConfig : InternalTopicConfig
    {
        public static IDictionary<string, string> WINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES = new Dictionary<string, string>(INTERNAL_TOPIC_DEFAULT_OVERRIDES)
        {
            { InternalTopicConfigCst.CLEANUP_POLICY_CONFIG, "compact,delete"}
        };

        private long retentionsMs = 0;

        public long RetentionMs
        {
            internal get
            {
                if (Configs != null && Configs.TryGetValue(InternalTopicConfigCst.RETENTION_MS_CONFIG, out var config))
                    return long.Parse(config);
                else
                    return retentionsMs;
            }
            set
            {
                if (Configs == null || !Configs.ContainsKey(InternalTopicConfigCst.RETENTION_MS_CONFIG))
                {
                    retentionsMs = value;
                }
            }
        }

        public override IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs)
        {
            IDictionary<string, string> topicConfig = new Dictionary<string, string>(WINDOWED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);
            topicConfig.AddRange(defaultConfigs);
            topicConfig.AddRange(Configs);

            long retentionValue = AddLong(RetentionMs, additionalRetentionMs);
            topicConfig.AddOrUpdate(InternalTopicConfigCst.RETENTION_MS_CONFIG, retentionValue.ToString());

            return topicConfig;
        }

        private static long AddLong(long value, long adder)
        {
            if (value > 0 && adder > long.MaxValue - value)
            {
                /* handle overflow */
                return long.MaxValue;
            }
            else if (value < 0 && adder < long.MaxValue - value)
            {
                /* handle underflow */
                return long.MinValue;
            }
            return value + adder;
        }
    }
}
