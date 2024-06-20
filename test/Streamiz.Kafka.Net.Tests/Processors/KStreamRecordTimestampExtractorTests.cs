using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamRecordTimestampExtractorTests
    {
        private class MyTimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                return Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow);
            }
        }

        [TestCase(0)]
        [TestCase(12345)]
        [TestCase(int.MaxValue)]
        public void StreamWithIngestionTimeTest(long customTimestamp)
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>() { KeyValuePair.Create("key1", "123456") };

            builder.Stream<string, string>("topic")
                .To((k,v,ctx) => "output-topic", (k,v,ctx) => customTimestamp);

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmap";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string, StringSerDes, StringSerDes>("output-topic");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 1);
                var record = result[0];
                Assert.AreEqual(customTimestamp, record.Message.Timestamp.UnixTimestampMs);
            }
        }

        [TestCase(0)]
        [TestCase(12345)]
        [TestCase(int.MaxValue)]
        public void StreamWithStreamTimestampExtractorTest(long customTimestamp)
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>() { KeyValuePair.Create("key1", "123456") };

            builder.Stream<string, string>("topic",
                new StringSerDes(),
                new StringSerDes(),
                new MyTimestampExtractor())
                .To((k, v, ctx) => "output-topic", (k, v, ctx) => customTimestamp);

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmap";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string, StringSerDes, StringSerDes>("output-topic");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 1);
                var record = result[0];
                Assert.AreEqual(customTimestamp, record.Message.Timestamp.UnixTimestampMs);
            }
        }

        [TestCase(0)]
        [TestCase(12345)]
        [TestCase(int.MaxValue)]
        public void WhenStreamWithOverriddenTimestamp(long customTimestamp)
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>() { KeyValuePair.Create("key1", "123456") };

            builder.Stream<string, string>("topic")
                .WithRecordTimestamp((k, v) => customTimestamp + 3600)
                .To((k, v, ctx) => "output-topic", (k, v, ctx) => customTimestamp);

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmap";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string, StringSerDes, StringSerDes>("output-topic");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 1);
                var record = result[0];
                Assert.AreEqual(customTimestamp, record.Message.Timestamp.UnixTimestampMs);
            }
        }
    }
}
