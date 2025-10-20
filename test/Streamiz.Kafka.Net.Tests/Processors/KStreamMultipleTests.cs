using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamMultipleTests
    {
        [Test]
        public void KStreamMultipleSink()
        {
            var builder = new StreamBuilder();

            var branches = builder.Stream<string, string>("topic")
                .Branch(
                    (k, v, _) => v.Length % 2 == 0,
                    (k, v, _) => v.Length % 2 > 0);
            
            branches[0].To("topic-B");
            branches[1].To("topic-C");
            
            builder
                .Stream<string, string>("topic-B")
                .MapValues((k,v, _) => v.ToUpper())
                .To("topic-D");

            builder
                .Stream<string, string>("topic-C")
                .MapValues((k,v, _) => v.ToLower())
                .To("topic-D");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-fix-181";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, string>("topic-D");
                inputTopic.PipeInput("key1", "value");
                inputTopic.PipeInput("key2", "value3");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.AreEqual(records["key1"], "value");
                Assert.AreEqual(records["key2"], "VALUE3");
            }
        }
    }
}