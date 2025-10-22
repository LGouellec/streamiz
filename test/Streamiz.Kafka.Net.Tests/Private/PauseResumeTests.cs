using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class PauseResumeTests
    {
        [Test]
        public void WorkflowCompleteMaxTaskIdleTest()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-group";
            config.MaxTaskIdleMs = (long) TimeSpan.FromSeconds(100).TotalMilliseconds;

            var builder = new StreamBuilder();

            var stream1 = builder.Stream<string, string>("topic1");
            var stream2 = builder.Stream<string, string>("topic2");

            stream1
                .LeftJoin(stream2, (v1, v2) => $"{v1}-{v2}", JoinWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOutputTopic<string, string>("output");
                inputTopic1.PipeInput("key", "i1");
                inputTopic1.PipeInput("key", "i2");
                inputTopic1.PipeInput("key", "i3");
                var items = outputTopic.ReadKeyValueList().ToList();
                Assert.AreEqual(0, items.Count);
            }
        }
    }
}