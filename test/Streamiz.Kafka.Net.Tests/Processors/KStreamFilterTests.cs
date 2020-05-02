using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamFilterTests
    {
        [Test]
        public void ShouldNotAllowNullFilterAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            Assert.Throws<ArgumentNullException>(() => stream.Filter(null));
        }

        [Test]
        public void FilterWithElements()
        {
            var builder = new StreamBuilder();
            var filterObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "car"));
            data.Add(KeyValuePair.Create("key3", "test"));

            builder.Stream<string, string>("topic")
                .Filter((k, v) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase))
                .Peek((k, v) => filterObserved.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInputs(data);

                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create("key1", "test1234"));
                expected.Add(KeyValuePair.Create("key3", "test"));

                Assert.AreEqual(expected, filterObserved);
            }
        }

        [Test]
        public void FilterNoElements()
        {
            var builder = new StreamBuilder();
            var filterObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key2", "car"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Stream<string, string>("topic")
                .Filter((k, v) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase))
                .Peek((k, v) => filterObserved.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInputs(data);
                Assert.AreEqual(new List<KeyValuePair<string, string>>(), filterObserved);
            }
        }

        [Test]
        public void FilterWithOneOutputElement()
        {
            var builder = new StreamBuilder();
            var filterObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "car"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Stream<string, string>("topic")
                .Filter((k, v) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase))
                .To("topic-filter");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-filter";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-filter");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValue();
                
                Assert.IsNotNull(result);
                Assert.AreEqual(result.Message.Key, "key1");
                Assert.AreEqual(result.Message.Value, "test1234");
            }
        }
    }
}
