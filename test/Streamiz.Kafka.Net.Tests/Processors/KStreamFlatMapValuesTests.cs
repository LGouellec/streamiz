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
    public class KStreamFlatMapValuesTests
    {
        [Test]
        public void ShouldNotAllowNullFlatmapAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");

            Func<string, IRecordContext, IEnumerable<string>> mapper1 = null;
            IValueMapper<string, IEnumerable<string>> mapper2 = null;
            IValueMapperWithKey<string, string, IEnumerable<string>> mapper3 = null;
            Func<string, string, IRecordContext, IEnumerable<string>> mapper4 = null;

            Assert.Throws<ArgumentNullException>(() => stream.FlatMapValues(mapper1));
            Assert.Throws<ArgumentNullException>(() => stream.FlatMapValues(mapper2));
            Assert.Throws<ArgumentNullException>(() => stream.FlatMapValues(mapper3));
            Assert.Throws<ArgumentNullException>(() => stream.FlatMapValues(mapper4));
        }

        [Test]
        public void FlatMapValuesSameType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .FlatMapValues((k, v, _) => v.ToCharArray().Select(c => c.ToString()))
                .To("topic-flatmapvalues");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmapvalues";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-flatmapvalues");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 6);
                for (int i = 1; i <= 6; ++i)
                {
                    Assert.AreEqual(result[i - 1].Message.Key, "key1");
                    Assert.AreEqual(result[i - 1].Message.Value, i.ToString());
                }
            }
        }

        [Test]
        public void FlatMapValuesOtherType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .FlatMapValues((k, v, _) => v.ToCharArray())
                .To<StringSerDes, CharSerDes>("topic-flatmap");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmap";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("topic-flatmap");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 6);
                for (int i = 1; i <= 6; ++i)
                {
                    Assert.AreEqual(result[i - 1].Message.Key, "key1");
                    Assert.AreEqual(result[i - 1].Message.Value, Convert.ToChar(i.ToString()));
                }
            }
        }
    }
}
