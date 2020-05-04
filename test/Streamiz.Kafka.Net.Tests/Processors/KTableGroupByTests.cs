using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableGroupByTests
    {
        [Test]
        public void SouldNotAllowSelectorNull()
        {
            var builder = new StreamBuilder();
            var table = builder.Table<string, string>("topic");
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

            var table = builder.Table<string, string>("topic");
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
    }
}
