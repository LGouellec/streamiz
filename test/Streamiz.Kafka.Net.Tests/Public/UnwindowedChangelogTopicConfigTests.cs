using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class UnwindowedChangelogTopicConfigTests
    {
        [Test]
        public void UnwindowedChangelogTopicConfigCtrComplete()
        {
            UnwindowedChangelogTopicConfig c = new UnwindowedChangelogTopicConfig()
            {
                Name = "topic1",
                NumberPartitions = 1,
                Configs = new Dictionary<string, string>()
            };

            Assert.AreEqual("topic1", c.Name);
            Assert.AreEqual(1, c.NumberPartitions);
            Assert.AreEqual(0, c.Configs.Count);
        }

        [Test]
        public void WindowedChangelogTopicGetProperties()
        {
            UnwindowedChangelogTopicConfig c = new UnwindowedChangelogTopicConfig()
            {
                Name = "topic1",
                NumberPartitions = 1,
                Configs = new Dictionary<string, string>()
                {
                    { "config1", "100"}
                }
            };

            var configs = c.GetProperties(new Dictionary<string, string>(), 120);

            Assert.AreEqual(3, configs.Count);
            Assert.AreEqual("100", configs["config1"]);
            Assert.AreEqual("CreateTime", configs[InternalTopicConfigCst.MESSAGE_TIMESTAMP_TYPE_CONFIG]);
            Assert.AreEqual("compact", configs[InternalTopicConfigCst.CLEANUP_POLICY_CONFIG]);
        }

        [Test]
        public void WindowedChangelogTopicGetPropertiesWithDefault()
        {
            UnwindowedChangelogTopicConfig c = new UnwindowedChangelogTopicConfig()
            {
                Name = "topic1",
                NumberPartitions = 1,
                Configs = new Dictionary<string, string>()
                {
                    { "config1", "100"}
                }
            };

            var configs = c.GetProperties(new Dictionary<string, string>() { { "default1", "test" } }, 120);

            Assert.AreEqual(4, configs.Count);
            Assert.AreEqual("test", configs["default1"]);
            Assert.AreEqual("100", configs["config1"]);
            Assert.AreEqual("CreateTime", configs[InternalTopicConfigCst.MESSAGE_TIMESTAMP_TYPE_CONFIG]);
            Assert.AreEqual("compact", configs[InternalTopicConfigCst.CLEANUP_POLICY_CONFIG]);
        }
    }
}
