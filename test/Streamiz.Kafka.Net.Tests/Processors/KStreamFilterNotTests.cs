using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamFilterNotTests
    {
        [Test]
        public void ShouldNotAllowNullFilterNotAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            Assert.Throws<ArgumentNullException>(() => stream.FilterNot(null));
        }

        [Test]
        public void FilterNotWithElements()
        {
            var builder = new StreamBuilder();
            var filterNotObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "car"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Stream<string, string>("topic")
                .FilterNot((k, v, _) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase))
                .Peek((k, v, _) => filterNotObserved.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-not-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInputs(data);

                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create("key2", "car"));
                expected.Add(KeyValuePair.Create("key3", "paper"));

                Assert.AreEqual(expected, filterNotObserved);
            }
        }

        [Test]
        public void FilterNotNoElements()
        {
            var builder = new StreamBuilder();
            var filterNotObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key2", "testddf"));
            data.Add(KeyValuePair.Create("key3", "test"));

            builder.Stream<string, string>("topic")
                .FilterNot((k, v, _) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase))
                .Peek((k, v, _) => filterNotObserved.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-not-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInputs(data);
                Assert.AreEqual(new List<KeyValuePair<string, string>>(), filterNotObserved);
            }
        }

        [Test]
        public void FilterNotWithOneOutputElement()
        {
            var builder = new StreamBuilder();
            var filterNotObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Stream<string, string>("topic")
                .FilterNot((k, v, _) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase))
                .To("topic-not-filter");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-not-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-not-filter");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValue();

                Assert.IsNotNull(result);
                Assert.AreEqual(result.Message.Key, "key3");
                Assert.AreEqual(result.Message.Value, "paper");
            }
        }

    }
}
