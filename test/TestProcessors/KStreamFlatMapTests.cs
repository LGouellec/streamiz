using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.TestProcessors
{
    public class KStreamFlatMapTests
    {
        [Test]
        public void ShouldNotAllowNullFlatmapAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            Func<string, string, IEnumerable<KeyValuePair<string, string>>> mapper1 = null;
            IKeyValueMapper<string, string, IEnumerable<KeyValuePair<string, string>>> mapper2 = null;
            
            Assert.Throws<ArgumentNullException>(() => stream.FlatMap(mapper1));
            Assert.Throws<ArgumentNullException>(() => stream.FlatMap(mapper2));
        }

        [Test]
        public void FlatMapOtherValueType()
        {
            var builder = new StreamBuilder();
            var filterObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .FlatMap((k, v) =>
                {
                    var results = new List<KeyValuePair<string, char>>();
                    var caracs = v.ToCharArray();
                    foreach (var c in caracs)
                        results.Add(KeyValuePair.Create(k, c));
                    return results;
                })
                .To<StringSerDes, CharSerDes>("topic-flatmap");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmap";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("topic-flatmap");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 6);
                for(int i = 1; i <= 6; ++i)
                {
                    Assert.AreEqual(result[i - 1].Message.Key, "key1");
                    Assert.AreEqual(result[i - 1].Message.Value, Convert.ToChar(i.ToString()));
                }
            }
        }

        [Test]
        public void FlatMapOtherKeyType()
        {
            var builder = new StreamBuilder();
            var filterObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .FlatMap((k, v) =>
                {
                    var results = new List<KeyValuePair<char, string>>();
                    var caracs = v.ToCharArray();
                    foreach (var c in caracs)
                        results.Add(KeyValuePair.Create(c, k));
                    return results;
                })
                .To<CharSerDes, StringSerDes>("topic-flatmap");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmap";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<char, string, CharSerDes, StringSerDes>("topic-flatmap");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 6);
                for (int i = 1; i <= 6; ++i)
                {
                    Assert.AreEqual(result[i - 1].Message.Key, Convert.ToChar(i.ToString()));
                    Assert.AreEqual(result[i - 1].Message.Value, "key1");
                }
            }
        }
    }
}
