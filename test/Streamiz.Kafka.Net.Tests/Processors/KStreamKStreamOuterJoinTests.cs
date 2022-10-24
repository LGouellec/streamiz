using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Linq;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamKStreamOuterJoinTests
    {
        class MyJoinerMapper : IValueJoiner<string, string, string>
        {
            public string Apply(string value1, string value2)
                => $"{value1}-{value2}";
        }

        [Test]
        public void StreamStreamOuterJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };
            config.UseRandomRocksDbConfigForTest();

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            builder
                .Stream<string, string>("topic2")
                .OuterJoin(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var records = outputTopic.ReadKeyValueList().ToList();
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("test", records[0].Message.Key);
                Assert.AreEqual("-test", records[0].Message.Value);
                Assert.AreEqual("test", records[1].Message.Key);
                Assert.AreEqual("coucou-test", records[1].Message.Value);
            }

            config.RemoveRocksDbFolderForTest();
        }

        [Test]
        public void StreamStreamOuterJoin2()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)));
            
            builder
                .Stream<string, string>("topic2")
                .OuterJoin<string, string, StringSerDes>(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var records = outputTopic.ReadKeyValueList().ToList();
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("test", records[0].Message.Key);
                Assert.AreEqual("-test", records[0].Message.Value);
                Assert.AreEqual("test", records[1].Message.Key);
                Assert.AreEqual("coucou-test", records[1].Message.Value);
            }
        }

        [Test]
        public void StreamStreamOuterJoin3()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)));
                    
            builder
                .Stream<string, string>("topic2")
                .OuterJoin<string, string, StringSerDes>(
                    stream,
                    new MyJoinerMapper(),
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var records = outputTopic.ReadKeyValueList().ToList();
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("test", records[0].Message.Key);
                Assert.AreEqual("-test", records[0].Message.Value);
                Assert.AreEqual("test", records[1].Message.Key);
                Assert.AreEqual("coucou-test", records[1].Message.Value);
            }
        }

        [Test]
        public void StreamStreamOuterJoin4()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)));
            
            builder
                .Stream<string, string>("topic2")
                .OuterJoin(
                    stream,
                    new MyJoinerMapper(),
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var records = outputTopic.ReadKeyValueList().ToList();
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("test", records[0].Message.Key);
                Assert.AreEqual("-test", records[0].Message.Value);
                Assert.AreEqual("test", records[1].Message.Key);
                Assert.AreEqual("coucou-test", records[1].Message.Value);
            }
        }

        [Test]
        public void StreamWithNullOuterStream()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            Assert.Throws<ArgumentNullException>(() => builder
               .Stream<string, string>("topic2")
               .OuterJoin(
                   null,
                   new MyJoinerMapper(),
                   JoinWindowOptions.Of(TimeSpan.FromSeconds(10))));
        }

        [Test]
        public void StreamWithNullOuterStream2()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            Assert.Throws<ArgumentNullException>(() => builder
               .Stream<string, string>("topic2")
               .OuterJoin(stream, (IValueJoiner<string, string, string>)null, JoinWindowOptions.Of(TimeSpan.FromSeconds(10))));
        }

        [Test]
        public void StreamSameStoreName()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var joinProps = StreamJoinProps.From<string, string, string>(StreamJoinProps.With(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("test", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("test", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10))));

            Assert.Throws<StreamsException>(() => builder
               .Stream<string, string>("topic2")
               .OuterJoin(stream, new MyJoinerMapper(), JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), joinProps));
        }

        [Test]
        public void StreamInvalidTimeSettings()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var joinProps = StreamJoinProps.From<string, string, string>(StreamJoinProps.With(
                State.Stores.InMemoryWindowStore("test1", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10)),
                State.Stores.InMemoryWindowStore("test2", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10))));

            // JoinWindowOptions.Of => use default retention => One Day
            // joinProps use supplier with retention 10 secondes => BAD THING !!
            Assert.Throws<StreamsException>(() => builder
               .Stream<string, string>("topic2")
               .OuterJoin(stream, new MyJoinerMapper(), JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), joinProps));
        }

        [Test]
        public void StreamStreamOuterJoinWithNoRecordInRigthJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)));
            
            builder
                .Stream<string, string>("topic2")
                .OuterJoin(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-", record.Message.Value);
            }
        }

        [Test]
        public void StreamStreamOuterJoinWithNoRecordInLeftJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var props = StreamJoinProps.With<string, string, string>(
                            Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-1-store", TimeSpan.FromDays(1),
                                TimeSpan.FromSeconds(10)),
                            Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-2-store", TimeSpan.FromDays(1),
                                TimeSpan.FromSeconds(10)));
                                
            builder
                .Stream<string, string>("topic2")
                .OuterJoin(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("-test", record.Message.Value);
            }
        }
    }
}
