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
    public class KTableGroupByTests
    {
        [Test]
        public void SouldNotAllowSelectorNull()
        {
            var builder = new StreamBuilder();
            var table = builder.Table<string, string>("topic", InMemory.As<string, string>("store"));
            Func<string, string, KeyValuePair<string,string>> selector1 = null;
            IKeyValueMapper<string, string, KeyValuePair<string, string>> selector2 = null;

            Assert.Throws<ArgumentNullException>(() => table.GroupBy(selector1));
            Assert.Throws<ArgumentNullException>(() => table.GroupBy(selector2));
        }

        [Test]
        public void TestGroupOK()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            var table = builder.Table<string, string>("topic", InMemory.As<string, string>("store"));
            table.GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()));
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-group";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInputs(data);
            }
        }
        
        [Test]
        public void TableGroupCachedTest()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test123"));
            data.Add(KeyValuePair.Create("key1", "test123"));
            data.Add(KeyValuePair.Create("key2", "test123"));
            data.Add(KeyValuePair.Create("key3", "test123"));
            data.Add(KeyValuePair.Create("key1", "test123"));
            data.Add(KeyValuePair.Create("key2", "test123"));
            
            var expected = new List<KeyValuePair<string, long>>();
            expected.Add(KeyValuePair.Create("KEY3", 1L));
            expected.Add(KeyValuePair.Create("KEY1", 1L));
            expected.Add(KeyValuePair.Create("KEY2", 1L));


            var table = builder
                .Table("topic",
                    InMemory
                        .As<string, string>("table-store"));
            
            table
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v.ToUpper()))
                .Count(InMemory
                    .As<string, long>("store-count")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes())
                    .WithCachingEnabled())
                .ToStream()
                .To("output", new StringSerDes(), new Int64SerDes());
            
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-group";

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