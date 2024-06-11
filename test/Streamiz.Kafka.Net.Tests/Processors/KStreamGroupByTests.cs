using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamGroupByTests
    {
        [Test]
        public void SouldNotAllowSelectorNullGroupBy()
        {
            var builder = new StreamBuilder();
            IKStream<string, string> stream = builder.Stream<string, string>("topic");
            Func<string, string, string> selector1 = null;
            IKeyValueMapper<string, string, string> selector2 = null;

            Assert.Throws<ArgumentNullException>(() => stream.GroupBy(selector1));
            Assert.Throws<ArgumentNullException>(() => stream.GroupBy(selector2));
            Assert.Throws<ArgumentNullException>(() => stream.GroupBy<string, StringSerDes>(selector1));
            Assert.Throws<ArgumentNullException>(() => stream.GroupBy<string, StringSerDes>(selector2));
        }

        [Test]
        public void GroupByKey()
        {
            var builder = new StreamBuilder();
            IKStream<string, string> stream = builder.Stream<string, string>("topic");

            stream.GroupByKey();
            stream.GroupByKey<StringSerDes, StringSerDes>();
        }

        [Test]
        public void TestGroupOK()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            var stream = builder.Stream<string, string>("topic");
            stream.GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()));
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-group";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInputs(data);
            }
        }

        [Test]
        public void TestGroupByKeyOK()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            var stream = builder.Stream<string, string>("topic");
            stream.GroupByKey();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-group";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInputs(data);
            }
        }

        [Test]
        public void StreamGroupCachedTest()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "1"));
            data.Add(KeyValuePair.Create("key2", "2"));
            data.Add(KeyValuePair.Create("key3", "3"));
            data.Add(KeyValuePair.Create("key2", "4"));
            data.Add(KeyValuePair.Create("key1", "5"));
            data.Add(KeyValuePair.Create("key1", "6"));
            data.Add(KeyValuePair.Create("key3", "7"));

            var expected = new List<KeyValuePair<string, long>>();
            expected.Add(KeyValuePair.Create("key2", 2L));
            expected.Add(KeyValuePair.Create("key1", 3L));
            expected.Add(KeyValuePair.Create("key3", 2L));
            
            var stream = builder.Stream<string, string>("topic");
            stream
                .GroupByKey()
                .Count(InMemory
                    .As<string, long>("store-count")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes())
                    .WithCachingEnabled())
                .ToStream()
                .To("output",new StringSerDes(), new Int64SerDes());
            
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-cached-stream-group";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>("output");
                
                inputTopic.PipeInputs(data);
                driver.Commit();
                
                var results = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value));
                Assert.AreEqual(expected, results);
            }
        }
    }
}
