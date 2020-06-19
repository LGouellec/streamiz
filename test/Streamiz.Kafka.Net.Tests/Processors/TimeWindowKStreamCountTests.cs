using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class TimeWindowKStreamCountTests
    {
        internal class StringTimeWindowedSerDes : TimeWindowedSerDes<string>
        {
            public StringTimeWindowedSerDes()
                : base(new StringSerDes() , (long)TimeSpan.FromSeconds(10).TotalMilliseconds)
            {

            }
        }

        [Test]
        public void TimeWindowingCount()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TimeWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .Count()
                .ToStream()
                .To<StringTimeWindowedSerDes, Int64SerDes>("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic("output", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int64SerDes());
                input.PipeInput("test", "1");
                input.PipeInput("test", "2");
                input.PipeInput("test", "3");
                var elements = output.ReadKeyValueList().ToList();
                Assert.AreEqual(3, elements.Count);
                Assert.AreEqual("test", elements[0].Message.Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(10).TotalMilliseconds, elements[0].Message.Key.Window.EndMs - elements[0].Message.Key.Window.StartMs);
                Assert.AreEqual(1, elements[0].Message.Value);
                Assert.AreEqual("test", elements[1].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[1].Message.Key.Window);
                Assert.AreEqual(2, elements[1].Message.Value);
                Assert.AreEqual("test", elements[2].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[2].Message.Key.Window);
                Assert.AreEqual(3, elements[2].Message.Value);
            }
        }

        [Test]
        public void TimeWindowingQueryStoreAll()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TimeWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .Count(InMemoryWindows<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", "2");
                input.PipeInput("test", "3");
                var store = driver.GetWindowStore<string, long>("count-store");
                var elements = store.All().ToList();
                Assert.AreEqual(1, elements.Count);
                Assert.AreEqual("test", elements[0].Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(10).TotalMilliseconds, elements[0].Key.Window.EndMs - elements[0].Key.Window.StartMs);
                Assert.AreEqual(3, elements[0].Value);
            }
        }
    }
}
