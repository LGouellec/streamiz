using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableKTableOuterJoinTests
    {
        internal class ValueJoiner : IValueJoiner<string, string, string>
        {
            public string Apply(string value1, string value2)
                => $"{value1}-{value2}";
        }

        [Test]
        public void TableTableOuterJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.OuterJoin(table2, (v1, v2) => $"{v1}-{v2}");

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                inputTopic1.PipeInput("test2", "test2");
                inputTopic2.PipeInput("test3", "test3");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(3, records.Count);
                Assert.AreEqual("test-coucou", records["test"]);
                Assert.AreEqual("test2-", records["test2"]);
                Assert.AreEqual("-test3", records["test3"]);
            }
        }

        [Test]
        public void TableTableOuterJoinValueJoiner()
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.OuterJoin(table2, new ValueJoiner());

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                inputTopic1.PipeInput("test2", "test2");
                inputTopic2.PipeInput("test3", "test3");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(3, records.Count);
                Assert.AreEqual("test-coucou", records["test"]);
                Assert.AreEqual("test2-", records["test2"]);
                Assert.AreEqual("-test3", records["test3"]);
            }
        }

        [Test]
        public void TableTableOuterJoinStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.OuterJoin(table2, (v1, v2) => $"{v1}-{v2}", InMemory<string, string>.As("merge-store"));

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");

                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test2", "test2");

                var st1 = driver.GetKeyValueStore<string, string>("store1");
                var st2 = driver.GetKeyValueStore<string, string>("store2");
                var mergeStore = driver.GetKeyValueStore<string, string>("merge-store");

                Assert.AreEqual(1, st1.ApproximateNumEntries());
                Assert.AreEqual(1, st2.ApproximateNumEntries());
                Assert.AreEqual(2, mergeStore.ApproximateNumEntries());

                Assert.AreEqual("test", st1.Get("test"));
                Assert.AreEqual("test2", st2.Get("test2"));
                Assert.AreEqual("test-", mergeStore.Get("test"));
                Assert.AreEqual("-test2", mergeStore.Get("test2"));
            }
        }

        [Test]
        public void TableTableOuterJoinWithtoutStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.OuterJoin(table2, (v1, v2) => $"{v1}-{v2}");

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                inputTopic1.PipeInput("test2", "test2");
                inputTopic2.PipeInput("test3", "test3");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(3, records.Count);
                Assert.AreEqual("test-coucou", records["test"]);
                Assert.AreEqual("test2-", records["test2"]);
                Assert.AreEqual("-test3", records["test3"]);
            }
        }

        [Test]
        public void TableTableOuterJoinNullKey()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.LeftJoin(table2, (v1, v2) => $"{v1}-{v2}");

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic1.PipeInput(null, "test");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual("test-", records["test"]);
            }
        }

        [Test]
        public void TableTableOuterJoinGetterSupplier()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("users", InMemory<string, string>.As("store-users"));
            var table2 = builder.Table("regions", InMemory<string, string>.As("store-regions"));
            var stream = builder.Stream<string, string>("orders");

            var tableJoin = table1.OuterJoin(table2, (v1, v2) => $"{v1 ?? "?"}-{v2 ?? "?"}");

            stream
                .Join(tableJoin,
                (order, ur) => $"Order:{order}|UserRegion:{ur}")
                .To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("users");
                var inputTopic2 = driver.CreateInputTopic<string, string>("regions");
                var inputTopic3 = driver.CreateInputTopic<string, string>("orders");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");

                inputTopic1.PipeInput("sylvain", "sylvain");
                inputTopic1.PipeInput("lise", "lise");
                inputTopic2.PipeInput("sylvain", "France");
                inputTopic2.PipeInput("remi", "USA");
                inputTopic3.PipeInput("sylvain", "iPhone12Pro");
                inputTopic3.PipeInput("lise", "PixelA4");
                inputTopic3.PipeInput("remi", "Tab");

                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(3, records.Count);
                Assert.AreEqual("Order:iPhone12Pro|UserRegion:sylvain-France", records["sylvain"]);
                Assert.AreEqual("Order:PixelA4|UserRegion:lise-?", records["lise"]);
                Assert.AreEqual("Order:Tab|UserRegion:?-USA", records["remi"]);
            }
        }

    }
}
