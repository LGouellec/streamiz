using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
                .Count(InMemoryWindows.As<string, long>("count-store"));

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
                .Count(InMemoryWindows.As<string, long>("count-store"));

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
                .Count(InMemory.As<string, long>("count-store"));

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
                .Reduce((v1, v2) => $"{v1}-{v2}", InMemory.As<string,string>())
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

        [Test]
        public void TestInputTopicHeaderPassthrough()
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>()
            {
                ApplicationId = "test-headers"
            };

            StreamBuilder builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .To("output");

            var topology = builder.Build();
            using var driver = new TopologyTestDriver(topology, config);
            var input = driver.CreateInputTopic<string, string>("topic");
            var outputTopic = driver.CreateOuputTopic<string, string>("output");

            var headers = new Headers
            {
                { "test-1-header", Encoding.UTF8.GetBytes("test-1-header") }
            };
            input.PipeInput("test-1", "test-ONE", headers);

            var output = outputTopic.ReadKeyValue();
            Assert.AreEqual(output.Message.Key, "test-1");
            Assert.AreEqual(output.Message.Value, "test-ONE");
            Assert.AreEqual(output.Message.Headers.Count, 1);
            Assert.AreEqual(Encoding.UTF8.GetString(output.Message.Headers[0].GetValueBytes()), "test-1-header");
        }

        [Test]
        public void TestMultiInputTopicHeaderPassthrough()
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>()
            {
                ApplicationId = "test-headers"
            };

            StreamBuilder builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .To("output");
            builder
                .Stream<string, string>("topic1")
                .To("output1");

            var topology = builder.Build();
            using var driver = new TopologyTestDriver(topology, config);
            var input = driver.CreateMultiInputTopic<string, string>("topic", "topic1");
            var outputTopic = driver.CreateOuputTopic<string, string>("output");
            var outputTopic1 = driver.CreateOuputTopic<string, string>("output1");

            var inputHeaders = new Headers
            {
                { "test-header", Encoding.UTF8.GetBytes("test-header") }
            };
            var input1Headers = new Headers
            {
                { "test-1-header", Encoding.UTF8.GetBytes("test-1-header") }
            };
            input.PipeInput("topic", "test", "test", inputHeaders);
            input.PipeInput("topic1", "test-1", "test-ONE", input1Headers);
            input.Flush();

            var output = outputTopic.ReadKeyValue();
            Assert.AreEqual(output.Message.Key, "test");
            Assert.AreEqual(output.Message.Value, "test");
            Assert.AreEqual(output.Message.Headers.Count, 1);
            Assert.AreEqual(Encoding.UTF8.GetString(output.Message.Headers[0].GetValueBytes()), "test-header");
            var output1 = outputTopic1.ReadKeyValue();
            Assert.AreEqual(output1.Message.Key, "test-1");
            Assert.AreEqual(output1.Message.Value, "test-ONE");
            Assert.AreEqual(output1.Message.Headers.Count, 1);
            Assert.AreEqual(Encoding.UTF8.GetString(output1.Message.Headers[0].GetValueBytes()), "test-1-header");
        }

        [Test]
        public void TestInputTopicHeaderAppendDuringProcessing()
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>()
            {
                ApplicationId = "test-headers"
            };

            StreamBuilder builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .To((k, v, ctx) =>
                {
                    ctx.Headers.Add("test-1-process_header", Encoding.UTF8.GetBytes("test-1-process_header"));
                    return "output";
                });

            var topology = builder.Build();
            using var driver = new TopologyTestDriver(topology, config);
            var input = driver.CreateInputTopic<string, string>("topic");
            var outputTopic = driver.CreateOuputTopic<string, string>("output");

            var inputHeaders = new Headers
            {
                { "test-1-header", Encoding.UTF8.GetBytes("test-1-header") }
            };
            input.PipeInput("test-1", "test-ONE", inputHeaders);

            var output = outputTopic.ReadKeyValue();
            Assert.AreEqual(output.Message.Key, "test-1");
            Assert.AreEqual(output.Message.Value, "test-ONE");
            Assert.AreEqual(output.Message.Headers.Count, 2);
            Assert.AreEqual(Encoding.UTF8.GetString(output.Message.Headers[0].GetValueBytes()), "test-1-header");
            Assert.AreEqual(Encoding.UTF8.GetString(output.Message.Headers[1].GetValueBytes()), "test-1-process_header");
        }

        [Test]
        public void TestMultiInputTopicHeaderAppendDuringProcessing()
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>()
            {
                ApplicationId = "test-headers"
            };

            StreamBuilder builder = new StreamBuilder();
            Func<string, string, IRecordContext, string, string> topicExtractor = (k, v, ctx, output) =>
            {
                ctx.Headers.Add($"test-{output}-process_header", Encoding.UTF8.GetBytes($"test-{output}-process_header"));
                return output;
            };
            builder
                .Stream<string, string>("topic")
                .To((k, v, ctx) => topicExtractor(k, v, ctx, "output"));
            builder
                .Stream<string, string>("topic1")
                .To((k, v, ctx) => topicExtractor(k, v, ctx, "output1"));

            var topology = builder.Build();
            using var driver = new TopologyTestDriver(topology, config);
            var input = driver.CreateMultiInputTopic<string, string>("topic", "topic1");
            var outputTopic = driver.CreateOuputTopic<string, string>("output");
            var outputTopic1 = driver.CreateOuputTopic<string, string>("output1");

            var inputHeaders = new Headers
            {
                { "test-header", Encoding.UTF8.GetBytes("test-header") }
            };
            var input1Headers = new Headers
            {
                { "test-1-header", Encoding.UTF8.GetBytes("test-1-header") }
            };
            input.PipeInput("topic", "test", "test", inputHeaders);
            input.PipeInput("topic1", "test-1", "test-ONE", input1Headers);
            input.Flush();

            var output = outputTopic.ReadKeyValue();
            Assert.AreEqual(output.Message.Key, "test");
            Assert.AreEqual(output.Message.Value, "test");
            Assert.AreEqual(output.Message.Headers.Count, 2);
            Assert.AreEqual(Encoding.UTF8.GetString(output.Message.Headers[0].GetValueBytes()), "test-header");
            Assert.AreEqual(Encoding.UTF8.GetString(output.Message.Headers[1].GetValueBytes()), "test-output-process_header");

            var output1 = outputTopic1.ReadKeyValue();
            Assert.AreEqual(output1.Message.Key, "test-1");
            Assert.AreEqual(output1.Message.Value, "test-ONE");
            Assert.AreEqual(output1.Message.Headers.Count, 2);
            Assert.AreEqual(Encoding.UTF8.GetString(output1.Message.Headers[0].GetValueBytes()), "test-1-header");
            Assert.AreEqual(Encoding.UTF8.GetString(output1.Message.Headers[1].GetValueBytes()), "test-output1-process_header");
        }
    }
}
