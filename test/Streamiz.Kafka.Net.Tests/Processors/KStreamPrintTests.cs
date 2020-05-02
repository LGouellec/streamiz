using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.IO;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamPrintTests
    {
        [Test]
        public void ShouldNotAllowNullAction()
        {
            var builder = new StreamBuilder();
            IKStream<string, string> stream = builder.Stream<string, string>("topic");
            Assert.Throws<ArgumentNullException>(() => stream.Print(null));
        }

        [Test]
        public void PrintElementWithWriter()
        {
            var builder = new StreamBuilder();
            var stringWriter = new StringWriter();

            builder.Stream<string, string>("topic")
                .Print(Printed<string, string>.ToWriter(stringWriter).WithLabel("string"));
            // DEFAULT FORMAT => [label]: key value";

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-print";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var expected = new StringBuilder();
                for (int i = 0; i < 5; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.AppendLine($"[string]: {key} {value}");
                }

                Assert.AreEqual(expected.ToString(), stringWriter.ToString());
            }
        }
    }
}
