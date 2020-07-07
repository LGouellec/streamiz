using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamTableLeftJoinTests
    {

        [Test]
        public void StreamTableLeftJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Table("test", InMemory<string, string>.As("store"));

            builder
                .Stream<string, string>("stream")
                .LeftJoin<string, string, StringSerDes, StringSerDes>(table, (s, v) => $"{s}-{v}")
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("test");
                var inputTopic2 = driver.CreateInputTopic<string, string>("stream");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                inputTopic2.PipeInput("test-sylvain", "1234");
                var record = outputTopic.ReadKeyValueList().ToList();
                Assert.IsNotNull(record);
                Assert.AreEqual(2, record.Count);
                Assert.AreEqual("test", record[0].Message.Key);
                Assert.AreEqual("coucou-test", record[0].Message.Value);
                Assert.AreEqual("test-sylvain", record[1].Message.Key);
                Assert.AreEqual("1234-", record[1].Message.Value);
            }
        }

        [Test]
        public void StreamTableLeftJoinWithGroupBy()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Stream<string, string>("test")
                            .GroupByKey()
                            .Reduce((v1, v2) => v2.Length > v1.Length ? v2 : v1);

            builder
                .Stream<string, string>("stream")
                .LeftJoin<string, string, StringSerDes, StringSerDes>(table, (s, v) => $"{s}-{v}")
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
                inputTopic2.PipeInput("sylvain", "sylvain");
                record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("sylvain", record.Message.Key);
                Assert.AreEqual("sylvain-", record.Message.Value);
            }
        }

        [Test]
        public void StreamTableLeftJoinBefore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table = builder
                            .Stream<string, string>("test")
                            .GroupByKey()
                            .Reduce((v1, v2) => v2.Length > v1.Length ? v2 : v1);

            builder
                .Stream<string, string>("stream")
                .LeftJoin<string, string, StringSerDes, StringSerDes>(table, (s, v) => $"{s}-{v}")
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
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-", record.Message.Value);
            }
        }
    }
}
