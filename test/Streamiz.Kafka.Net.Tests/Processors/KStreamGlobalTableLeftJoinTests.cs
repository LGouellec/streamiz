using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamGlobalTableLeftJoinTests
    {
        class MyKeyValueMapper : IKeyValueMapper<string, string, string>
        {
            public string Apply(string key, string value)
                => key;
        }

        class MyJoinValueMapper : IValueJoiner<string, string, string>
        {
            public string Apply(string value1, string value2)
               => $"{value1}-{value2}";
        }

        [Test]
        public void KStreamGlobalLeftJoinOK()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var global = builder.GlobalTable("global", InMemory.As<string,string>("global-store"));

            builder
                .Stream<string, string>("stream")
                .LeftJoin(global, (k, v) => k, (s, v) => $"{s}-{v}")
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("global");
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
        public void KStreamGlobalLeftJoinOK2()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var global = builder.GlobalTable("global", InMemory.As<string,string>("global-store"));

            builder
                .Stream<string, string>("stream")
                .LeftJoin(global, new MyKeyValueMapper(), new MyJoinValueMapper())
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("global");
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
        public void KStreamGlobalLeftJoinNull()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-table-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var global = builder.GlobalTable("global", InMemory.As<string,string>("global-store"));

            builder
                .Stream<string, string>("stream")
                .LeftJoin(global, (k, v) => k, (s, v) => $"{s}-{v}")
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("global");
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
