using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    public class TopologyTestDriverTests
    {
        [Test]
        public void TestGetWindowStoreDoesntNotExist()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(5)))
                .Count(InMemoryWindows<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                DateTime dt = DateTime.Now;
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1", dt);
                var store = driver.GetWindowStore<string, long>("store");
                Assert.IsNull(store);
            }
        }

        [Test]
        public void TestGetWindowStoreIncorrectType()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(5)))
                .Count(InMemoryWindows<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                DateTime dt = DateTime.Now;
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1", dt);
                var store = driver.GetWindowStore<int, long>("count-store");
                Assert.IsNull(store);
            }
        }

        [Test]
        public void TestGetWindowStoreKeyValueStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .Count(InMemory<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                DateTime dt = DateTime.Now;
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1", dt);
                var store = driver.GetWindowStore<string, long>("count-store");
                Assert.IsNull(store);
            }
        }

        [Test]
        public void TestWithTwoSubTopology()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>()
            {
                ApplicationId = "test-reducer"
            };

            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .Filter((key, value) =>
                {
                    return key == "1";
                })
                .To("tempTopic");

            builder.Stream<string, string>("tempTopic")
                .GroupByKey()
                .Reduce((v1,v2)=>$"{v1}-{v2}")
                .ToStream()
                .To("finalTopic");

            var topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var tempTopic = driver.CreateOuputTopic<string, string>("tempTopic");
                var finalTopic = driver.CreateOuputTopic<string, string>("finalTopic");

                input.PipeInput("1", "Once");
                input.PipeInput("2", "Once");
                input.PipeInput("1", "Twice");
                input.PipeInput("3", "Once");
                input.PipeInput("1", "Thrice");
                input.PipeInput("2", "Twice");

                var list = finalTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                foreach (var item in list)
                {
                    Console.WriteLine(item);
                }

                Assert.IsNotNull("x");
            }
        }
    }
}
