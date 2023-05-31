using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamWithRecordTimestampTests
    {
        [Test]
        public void ShouldNotAllowNullTimestampExtractorAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");

            Assert.Throws<ArgumentNullException>(() => stream.WithRecordTimestamp(null));
        }

        [Test]
        public void ShouldIgnoreNegativeTimestamp()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            var negativeTimestamp = -12345;

            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .WithRecordTimestamp((k, v) => negativeTimestamp)
                .To("output-topic");

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
                Assert.IsTrue(record.Message.Timestamp.UnixTimestampMs > 0);
            }
        }

        [TestCase(0)]
        [TestCase(12345)]
        [TestCase(int.MaxValue)]
        public void ShouldReturnMessageWithExtractedTimestamp(long customTimestamp)
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();

            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .WithRecordTimestamp((k, v) => customTimestamp)
                .To("output-topic");

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
