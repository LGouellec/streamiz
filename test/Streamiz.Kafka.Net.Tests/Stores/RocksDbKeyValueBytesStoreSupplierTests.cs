using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class RocksDbKeyValueBytesStoreSupplierTests
    {
        [Test]
        public void KTableSource()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", RocksDb<string, string>.As<StringSerDes, StringSerDes>("table-topic-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";
            config.UseRandomRocksDbConfigForTest();

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInput("key1", "1");
                inputTopic.PipeInput("key2", "2");

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                var resultK1 = store.Get("key1");
                var resultK2 = store.Get("key2");

                Assert.AreEqual("1", resultK1);
                Assert.AreEqual("2", resultK2);
            }
        }
    }
}
