using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamMergeTests
    {
        [Test]
        public void MergeTwoStreamsWithElements()
        {
            var data1 = new List<KeyValuePair<string, int>>
            {
                KeyValuePair.Create("key1", 1),
                KeyValuePair.Create("key2", 2)
            };

            var data2 = new List<KeyValuePair<string, int>>
            {
                KeyValuePair.Create("key3", 3)
            };

            var observed = new List<KeyValuePair<string, int>>();

            var builder = new StreamBuilder();
            var stream1 = builder.Stream<string, int>("topic1"); 
            var stream2 = builder.Stream<string, int>("topic2");
            var merged = stream1.Merge(stream2);
            merged.Peek((k, v, _) => observed.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, Int32SerDes>
            {
                ApplicationId = "test-merge"
            };

            Topology t = builder.Build();

            using var driver = new TopologyTestDriver(t, config);
            var inputTopic1 = driver.CreateInputTopic<string, int>("topic1");
            inputTopic1.PipeInputs(data1);

            var inputTopic2 = driver.CreateInputTopic<string, int>("topic2");
            inputTopic2.PipeInputs(data2);

            var expected = new List<KeyValuePair<string, int>>();
            expected.AddRange(data1);
            expected.AddRange(data2);

            Assert.AreEqual(expected, observed);
        }

        [Test]
        public void MergeStreamWithElementsAndEmptyStream()
        {
            var data1 = new List<KeyValuePair<string, int>>
            {
                KeyValuePair.Create("key1", 1),
                KeyValuePair.Create("key2", 2)
            };

            var data2 = new List<KeyValuePair<string, int>>();

            var observed = new List<KeyValuePair<string, int>>();

            var builder = new StreamBuilder();
            var stream1 = builder.Stream<string, int>("topic1");
            var stream2 = builder.Stream<string, int>("topic2");
            var merged = stream1.Merge(stream2);
            merged.Peek((k, v, _) => observed.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, Int32SerDes>
            {
                ApplicationId = "test-merge"
            };

            Topology t = builder.Build();

            using var driver = new TopologyTestDriver(t, config);
            var inputTopic1 = driver.CreateInputTopic<string, int>("topic1");
            inputTopic1.PipeInputs(data1);

            var inputTopic2 = driver.CreateInputTopic<string, int>("topic2");
            inputTopic2.PipeInputs(data2);

            var expected = new List<KeyValuePair<string, int>>();
            expected.AddRange(data1);
            expected.AddRange(data2);

            Assert.AreEqual(expected, observed);
        }

        [Test]
        public void MergeEmptyStreamAndStreamWithElements()
        {
            var data1 = new List<KeyValuePair<string, int>>();

            var data2 = new List<KeyValuePair<string, int>>
            {
                KeyValuePair.Create("key3", 3)
            };

            var observed = new List<KeyValuePair<string, int>>();

            var builder = new StreamBuilder();
            var stream1 = builder.Stream<string, int>("topic1");
            var stream2 = builder.Stream<string, int>("topic2");
            var merged = stream1.Merge(stream2);
            merged.Peek((k, v, _) => observed.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, Int32SerDes>
            {
                ApplicationId = "test-merge"
            };

            Topology t = builder.Build();

            using var driver = new TopologyTestDriver(t, config);
            var inputTopic1 = driver.CreateInputTopic<string, int>("topic1");
            inputTopic1.PipeInputs(data1);

            var inputTopic2 = driver.CreateInputTopic<string, int>("topic2");
            inputTopic2.PipeInputs(data2);

            var expected = new List<KeyValuePair<string, int>>();
            expected.AddRange(data1);
            expected.AddRange(data2);

            Assert.AreEqual(expected, observed);
        }
    }
}
