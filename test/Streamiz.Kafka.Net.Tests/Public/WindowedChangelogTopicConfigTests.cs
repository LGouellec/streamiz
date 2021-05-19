using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class WindowedChangelogTopicConfigTests
    {
        [Test]
        public void WindowedChangelogTopicConfigCtrComplete()
        {
            WindowedChangelogTopicConfig c = new WindowedChangelogTopicConfig()
            {
                Name = "topic1",
                NumberPartitions = 1,
                RetentionMs = 10,
                Configs = new Dictionary<string, string>()
            };

            Assert.AreEqual("topic1", c.Name);
            Assert.AreEqual(1, c.NumberPartitions);
            Assert.AreEqual(10, c.RetentionMs);
            Assert.AreEqual(0, c.Configs.Count);
        }

        [Test]
        public void WindowedChangelogTopicConfigRetention()
        {
            WindowedChangelogTopicConfig c = new WindowedChangelogTopicConfig()
            {
                Name = "topic1",
                NumberPartitions = 1,
                Configs = new Dictionary<string, string>()
                {
                    { InternalTopicConfigCst.RETENTION_MS_CONFIG, "100"}
                }
            };
            c.RetentionMs = 50;

            Assert.AreEqual("topic1", c.Name);
            Assert.AreEqual(1, c.NumberPartitions);
            Assert.AreEqual(100, c.RetentionMs);
            Assert.AreEqual(1, c.Configs.Count);
        }

        [Test]
        public void WindowedChangelogTopicGetProperties()
        {
            WindowedChangelogTopicConfig c = new WindowedChangelogTopicConfig()
            {
                Name = "topic1",
                NumberPartitions = 1,
                Configs = new Dictionary<string, string>()
                {
                    { "config1", "100"}
                },
                RetentionMs = 50
            };

            var configs = c.GetProperties(new Dictionary<string, string>(), 120);

            Assert.AreEqual(4, configs.Count);
            Assert.AreEqual("170", configs[InternalTopicConfigCst.RETENTION_MS_CONFIG]);
            Assert.AreEqual("100", configs["config1"]);
            Assert.AreEqual("CreateTime", configs[InternalTopicConfigCst.MESSAGE_TIMESTAMP_TYPE_CONFIG]);
            Assert.AreEqual("compact,delete", configs[InternalTopicConfigCst.CLEANUP_POLICY_CONFIG]);
        }

        [Test]
        public void WindowedChangelogTopicGetPropertiesTooBigLonger()
        {
            WindowedChangelogTopicConfig c = new WindowedChangelogTopicConfig()
            {
                Name = "topic1",
                NumberPartitions = 1,
                Configs = new Dictionary<string, string>(),
                RetentionMs = long.MaxValue - 3
            };

            var configs = c.GetProperties(new Dictionary<string, string>(), 5);

            Assert.AreEqual(3, configs.Count);
            Assert.AreEqual(long.MaxValue.ToString(), configs[InternalTopicConfigCst.RETENTION_MS_CONFIG]);
            Assert.AreEqual("CreateTime", configs[InternalTopicConfigCst.MESSAGE_TIMESTAMP_TYPE_CONFIG]);
            Assert.AreEqual("compact,delete", configs[InternalTopicConfigCst.CLEANUP_POLICY_CONFIG]);
        }

        [Test]
        public void WindowedChangelogTopicGetPropertiesWithDefault()
        {
            WindowedChangelogTopicConfig c = new WindowedChangelogTopicConfig()
            {
                Name = "topic1",
                NumberPartitions = 1,
                Configs = new Dictionary<string, string>()
                {
                    { "config1", "100"}
                },
                RetentionMs = 50
            };

            var configs = c.GetProperties(new Dictionary<string, string>() { { "default1", "test" } }, 120);

            Assert.AreEqual(5, configs.Count);
            Assert.AreEqual("test", configs["default1"]);
            Assert.AreEqual("170", configs[InternalTopicConfigCst.RETENTION_MS_CONFIG]);
            Assert.AreEqual("100", configs["config1"]);
            Assert.AreEqual("CreateTime", configs[InternalTopicConfigCst.MESSAGE_TIMESTAMP_TYPE_CONFIG]);
            Assert.AreEqual("compact,delete", configs[InternalTopicConfigCst.CLEANUP_POLICY_CONFIG]);
        }
    }
}
