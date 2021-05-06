using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KGlobalTableTests
    {
        [Test]
        public void GlobalTableWithStoreQueryable()
        {
            var builder = new StreamBuilder();

            var table = builder.GlobalTable("topic", InMemory<string, string>.As("global-store"));
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-globaltable";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("test", "coucou");
                var store = driver.GetKeyValueStore<string, string>("global-store");
                var ele = store.Get("test");
                Assert.IsNotNull(ele);
                Assert.AreEqual("coucou", ele);
            }
        }

        [Test]
        public void GlobalTableWithStoreQueryableWithRocksDb()
        {
            var builder = new StreamBuilder();

            var table = builder.GlobalTable("topic", RocksDb<string, string>.As("global-store"));
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-globaltable";
            config.UseRandomRocksDbConfigForTest();

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("test", "coucou");
                var store = driver.GetKeyValueStore<string, string>("global-store");
                var ele = store.Get("test");
                Assert.IsNotNull(ele);
                Assert.AreEqual("coucou", ele);
            }
            config.RemoveRocksDbFolderForTest();
        }
    }
}
