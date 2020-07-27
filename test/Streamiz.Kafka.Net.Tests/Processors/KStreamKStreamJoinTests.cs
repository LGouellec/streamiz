using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamKStreamJoinTests
    {
        [Test]
        public void StreamStreamJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            builder
                .Stream<string, string>("topic2")
                .Join<string, string, StringSerDes, StringSerDes>(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }

    }
}
