using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableKTableJoinTests
    {
        internal class ValueJoiner : IValueJoiner<string, string, string>
        {
            public string Apply(string value1, string value2)
                => $"{value1}-{value2}";
        }

        [Test]
        public void TableTableJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory.As<string,string>("store1"));
            var table2 = builder.Table("table2", InMemory.As<string,string>("store2"));

            var tableJoin = table1.Join(table2, (v1, v2) => $"{v1}-{v2}");

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("test-coucou", record.Message.Value);
            }
        }

        [Test]
        public void TableTableJoinValueJoiner()
        {
          
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory.As<string,string>("store1"));
            var table2 = builder.Table("table2", InMemory.As<string,string>("store2"));

            var tableJoin = table1.Join(table2, new ValueJoiner());

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("test-coucou", record.Message.Value);
            }
        }

        [Test]
        public void TableTableJoinStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory.As<string,string>("store1"));
            var table2 = builder.Table("table2", InMemory.As<string,string>("store2"));

            var tableJoin = table1.Join(table2, (v1, v2) => $"{v1}-{v2}", InMemory.As<string,string>("merge-store"));

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");

                var st1 = driver.GetKeyValueStore<string, string>("store1");
                var st2 = driver.GetKeyValueStore<string, string>("store2");
                var mergeStore = driver.GetKeyValueStore<string, string>("merge-store");

                Assert.AreEqual(1, st1.ApproximateNumEntries());
                Assert.AreEqual(1, st2.ApproximateNumEntries());
                Assert.AreEqual(1, mergeStore.ApproximateNumEntries());

                Assert.AreEqual("test", st1.Get("test"));
                Assert.AreEqual("coucou", st2.Get("test"));
                Assert.AreEqual("test-coucou", mergeStore.Get("test"));
            }
        }

        [Test]
        public void TableTableJoinWithtoutStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory.As<string,string>("store1"));
            var table2 = builder.Table("table2", InMemory.As<string,string>("store2"));

            var tableJoin = table1.Join(table2, (v1, v2) => $"{v1}-{v2}");
            
            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("test-coucou", record.Message.Value);
            }

        }

        [Test]
        public void TableTableJoinNullKey()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory.As<string,string>("store1"));
            var table2 = builder.Table("table2", InMemory.As<string,string>("store2"));

            var tableJoin = table1.Join(table2, (v1, v2) => $"{v1}-{v2}");

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic1.PipeInput(null, "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("test-coucou", record.Message.Value);
            }
        }

        [Test]
        public void TableTableJoinGetterSupplier()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("users", InMemory.As<string,string>("store-users"));
            var table2 = builder.Table("regions", InMemory.As<string,string>("store-regions"));
            var stream = builder.Stream<string, string>("orders");

            var tableJoin = table1.Join(table2, (v1, v2) => $"{v1}-{v2}");

            stream
                .Join(tableJoin, (order, ur) => $"Order:{order}|UserRegion:{ur}")
                .To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("users");
                var inputTopic2 = driver.CreateInputTopic<string, string>("regions");
                var inputTopic3 = driver.CreateInputTopic<string, string>("orders");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-output");
                
                inputTopic1.PipeInput("sylvain", "sylvain");
                inputTopic2.PipeInput("sylvain", "France");
                inputTopic3.PipeInput("sylvain", "iPhone12Pro");

                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("sylvain", record.Message.Key);
                Assert.AreEqual("Order:iPhone12Pro|UserRegion:sylvain-France", record.Message.Value);
            }
        }

    }
}
