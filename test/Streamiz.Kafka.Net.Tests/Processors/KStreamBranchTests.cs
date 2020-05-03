using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamBranchTests
    {
        [Test]
        public void ShouldNotAllowNullBranchFunction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, int>("topic");
            Assert.Throws<ArgumentNullException>(() => stream.Branch("branch", null));
        }

        [Test]
        public void ShouldNotAllowEmptyBranchFunction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, int>("topic");
            Assert.Throws<ArgumentException>(() => stream.Branch());
        }

        [Test]
        public void OneBranchWithElements()
        {
            var builder = new StreamBuilder();

            var branchs = builder.Stream<string, string>("topic")
                .Branch((k, b) => true);

            branchs[0].To("topic-branch0");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-branch";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-branch0");

                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 10; i++)
                {
                    string key = i.ToString(), value = $"value-{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }

                var list = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expected, list);
            }
        }

        [Test]
        public void MultiBranchWithElements()
        {
            var builder = new StreamBuilder();

            var branchs = builder.Stream<string, int>("topic")
                .Branch((k, v) => v % 2 == 0, (k, v) => v % 2 > 0);

            branchs[0].To("topic-pair");
            branchs[1].To("topic-impair");

            var config = new StreamConfig<StringSerDes, Int32SerDes>();
            config.ApplicationId = "test-branch";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, int>("topic");
                var outputTopicPair = driver.CreateOuputTopic<string, int>("topic-pair");
                var outputTopicImpair = driver.CreateOuputTopic<string, int>("topic-impair");

                var expectedPair = new List<KeyValuePair<string, int>>();
                var expectedImpair = new List<KeyValuePair<string, int>>();
                for (int i = 0; i < 10; i++)
                {
                    string key = i.ToString();
                    int value = i;
                    inputTopic.PipeInput(key, value);

                    if(i%2 == 0)
                        expectedPair.Add(KeyValuePair.Create(key, value));
                    else
                        expectedImpair.Add(KeyValuePair.Create(key, value));
                }

                var listPair = outputTopicPair.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();
                var listImpair = outputTopicImpair.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expectedPair, listPair);
                Assert.AreEqual(expectedImpair, listImpair);
            }
        }
    }
}
