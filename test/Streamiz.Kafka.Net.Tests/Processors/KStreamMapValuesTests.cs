using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamMapValuesTests
    {
        [Test]
        public void ShouldNotAllowNullFlatmapAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");

            Func<string, IRecordContext, string> mapper1 = null;
            IValueMapper<string, string> mapper2 = null;
            IValueMapperWithKey<string, string, string> mapper3 = null;
            Func<string, string, IRecordContext, string> mapper4 = null;

            Assert.Throws<ArgumentNullException>(() => stream.MapValues(mapper1));
            Assert.Throws<ArgumentNullException>(() => stream.MapValues(mapper2));
            Assert.Throws<ArgumentNullException>(() => stream.MapValues(mapper3));
            Assert.Throws<ArgumentNullException>(() => stream.MapValues(mapper4));
        }

        [Test]
        public void MapValuesSameType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "abc"));
            data.Add(KeyValuePair.Create("key2", "test"));

            builder.Stream<string, string>("topic")
                .MapValues((k, v, _) => v.ToUpper())
                .To("topic-mapvalues");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-mapvalues";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-mapvalues");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create("key1", "ABC"));
                expected.Add(KeyValuePair.Create("key2", "TEST"));

                Assert.IsNotNull(result);
                Assert.AreEqual(expected, result);
            }
        }

        [Test]
        public void MapValuesOtherType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .MapValues((k, v, _) => v.Length)
                .To<StringSerDes, Int32SerDes>("topic-mapvalues");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-mapvalues";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, int, StringSerDes, Int32SerDes>("topic-mapvalues");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValue();

                Assert.IsNotNull(result);
                Assert.AreEqual(result.Message.Key, "key1");
                Assert.AreEqual(result.Message.Value, 6);
            }
        }
    }
}