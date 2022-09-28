using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamTableJoinTests
    {
        class MyJoinValueMapper : IValueJoiner<string, string, string>
        {
            public string Apply(string value1, string value2)
                => $"{value1}-{value2}";
        }

        [Test]
        public void StreamTableJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Table("test", InMemory.As<string, string>("store"));

            builder
                .Stream<string, string>("stream")
                .Join<string, string, StringSerDes>(table, (s, v) => $"{s}-{v}")
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var inputTopic2 = driver.CreateInputTopic<string, string>("stream");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }

        [Test]
        public void StreamTableJoin2()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Table("test", InMemory.As<string, string>("store"));

            builder
                .Stream<string, string>("stream")
                .Join<string, string, StringSerDes>(table, new MyJoinValueMapper())
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var inputTopic2 = driver.CreateInputTopic<string, string>("stream");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }


        [Test]
        public void StreamTableJoin3()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Table("test", InMemory.As<string, string>("store"));

            builder
                .Stream<string, string>("stream")
                .Join(table, new MyJoinValueMapper())
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var inputTopic2 = driver.CreateInputTopic<string, string>("stream");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }

        [Test]
        public void StreamTableJoinWithGroupBy()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Stream<string, string>("test")
                            .GroupByKey()
                            .Reduce((v1, v2) => v2.Length > v1.Length ? v2 : v1);

            builder
                .Stream<string, string>("stream")
                .Join(table, (s, v) => $"{s}-{v}")
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var inputTopic2 = driver.CreateInputTopic<string, string>("stream");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }

        [Test]
        public void StreamTableJoinImpossible()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Stream<string, string>("test")
                            .GroupByKey()
                            .Reduce((v1, v2) => v2.Length > v1.Length ? v2 : v1);

            builder
                .Stream<string, string>("stream")
                .Join(table, (s, v) => $"{s}-{v}")
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var inputTopic2 = driver.CreateInputTopic<string, string>("stream");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic2.PipeInput("test", "coucou");
                inputTopic.PipeInput("test", "test");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNull(record);
            }
        }
    }
}
