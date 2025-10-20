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
    public class KStreamSelectKeyTests
    {
        [Test]
        public void ShouldNotAllowNullFlatmapAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            Func<string, string, IRecordContext, string> mapper1 = null;
            IKeyValueMapper<string, string, string> mapper2 = null;

            Assert.Throws<ArgumentNullException>(() => stream.SelectKey(mapper1));
            Assert.Throws<ArgumentNullException>(() => stream.SelectKey(mapper2));
        }

        [Test]
        public void SelectKeyChangeType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "SO"));
            data.Add(KeyValuePair.Create("asmrugby", "2"));
            data.Add(KeyValuePair.Create("toulon", "10"));

            builder.Stream<string, string>("topic")
                .SelectKey((k,v, _) => k.Length)
                .To<Int32SerDes, StringSerDes>("topic-select-key");

            var expected = new List<KeyValuePair<int, string>>();
            expected.Add(KeyValuePair.Create(4, "SO"));
            expected.Add(KeyValuePair.Create(8, "2"));
            expected.Add(KeyValuePair.Create(6, "10"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-select-key";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<int, string, Int32SerDes, StringSerDes>("topic-select-key");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.IsNotNull(result);
                Assert.AreEqual(expected, result);
            }
        }

        [Test]
        public void SelectKeySameType()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "SO"));
            data.Add(KeyValuePair.Create("asmrugby", "2"));
            data.Add(KeyValuePair.Create("toulon", "10"));

            builder.Stream<string, string>("topic")
                .SelectKey((k, v, _) => k.ToUpper())
                .To("topic-select-key");

            var expected = new List<KeyValuePair<string, string>>();
            expected.Add(KeyValuePair.Create("KEY1", "SO"));
            expected.Add(KeyValuePair.Create("ASMRUGBY", "2"));
            expected.Add(KeyValuePair.Create("TOULON", "10"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-select-key";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-select-key");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.IsNotNull(result);
                Assert.AreEqual(expected, result);
            }
        }
    }
}
