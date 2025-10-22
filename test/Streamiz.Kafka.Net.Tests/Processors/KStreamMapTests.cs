using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamMapTests
    {
        [Test]
        public void ShouldNotAllowNullmapAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            Func<string, string, IRecordContext,  KeyValuePair<string, string>> mapper1 = null;
            IKeyValueMapper<string, string, KeyValuePair<string, string>> mapper2 = null;

            Assert.Throws<ArgumentNullException>(() => stream.Map(mapper1));
            Assert.Throws<ArgumentNullException>(() => stream.Map(mapper2));
        }

        [Test]
        public void MapOtherValueType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .Map((k, v, _) => KeyValuePair.Create(k, v.Length))
                .To<StringSerDes, Int32SerDes>("topic-map");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, int, StringSerDes, Int32SerDes>("topic-map");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValue();

                Assert.IsNotNull(result);
                Assert.AreEqual(result.Message.Key, "key1");
                Assert.AreEqual(result.Message.Value, 6);
            }
        }

        [Test]
        public void MapOtherKeyType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .Map((k, v, _) => KeyValuePair.Create(v.Length, k))
                .To<Int32SerDes, StringSerDes>("topic-map");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<int, string, Int32SerDes, StringSerDes>("topic-map");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValue();

                Assert.IsNotNull(result);
                Assert.AreEqual(result.Message.Key, 6);
                Assert.AreEqual(result.Message.Value, "key1");
            }
        }

        [Test]
        public void MapSameValueType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "abc"));

            builder.Stream<string, string>("topic")
                .Map((k, v, _) => KeyValuePair.Create(k, v.ToUpper()))
                .To<StringSerDes, StringSerDes>("topic-map");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, string, StringSerDes, StringSerDes>("topic-map");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValue();

                Assert.IsNotNull(result);
                Assert.AreEqual(result.Message.Key, "key1");
                Assert.AreEqual(result.Message.Value, "ABC");
            }
        }
    }
}
