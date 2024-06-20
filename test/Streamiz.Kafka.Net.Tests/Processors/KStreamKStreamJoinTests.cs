using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamKStreamJoinTests
    {
        class MyJoinerMapper : IValueJoiner<string, string, string>
        {
            public string Apply(string value1, string value2)
                => $"{value1}-{value2}";
        }

        [Test]
        public void StreamStreamJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
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
                .Join(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)),
                    props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }

        [Test]
        public void StreamStreamJoin2()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
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
                .Join<string, string, StringSerDes, StringSerDes>(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)),
                    props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }

        [Test]
        public void StreamStreamJoin3()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
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
                .Join<string, string, StringSerDes, StringSerDes>(
                    stream,
                    new MyJoinerMapper(),
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)),
                    props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }

        [Test]
        public void StreamStreamJoin4()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
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
                .Join(
                    stream,
                    new MyJoinerMapper(),
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)),
                    props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test", record.Message.Key);
                Assert.AreEqual("coucou-test", record.Message.Value);
            }
        }

        [Test]
        public void StreamWithNullStream()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)));
            
            Assert.Throws<ArgumentNullException>(() => builder
               .Stream<string, string>("topic2")
               .Join(
                   null,
                   new MyJoinerMapper(),
                   JoinWindowOptions.Of(TimeSpan.FromSeconds(5)),props));
        }

        [Test]
        public void StreamWithNullStream2()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(10)));
            
            Assert.Throws<ArgumentNullException>(() => builder
               .Stream<string, string>("topic2")
               .Join(stream, (IValueJoiner<string, string, string>)null, JoinWindowOptions.Of(TimeSpan.FromSeconds(5)),props));
        }

        [Test]
        public void StreamSameStoreName()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var joinProps = StreamJoinProps.From<string, string, string>(StreamJoinProps.With(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("test", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("test", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10))));

            Assert.Throws<StreamsException>(() => builder
               .Stream<string, string>("topic2")
               .Join(stream, new MyJoinerMapper(), JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), joinProps));
        }

        [Test]
        public void StreamInvalidTimeSettings()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("topic1");

            var joinProps = StreamJoinProps.From<string, string, string>(StreamJoinProps.With(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("test1", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("test2", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10))));

            // JoinWindowOptions.Of => use default retention => One Day
            // joinProps use supplier with retention 10 secondes => BAD THING !!
            Assert.Throws<StreamsException>(() => builder
               .Stream<string, string>("topic2")
               .Join(stream, new MyJoinerMapper(), JoinWindowOptions.Of(TimeSpan.FromSeconds(5)), joinProps));
        }

        [Test]
        public void StreamStreamJoinWithNoRecordInRigthJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
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
                .Join(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)),
                    props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");
                inputTopic2.PipeInput("test", "coucou");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNull(record);
            }
        }

        [Test]
        public void StreamStreamJoinWithNoRecordInLeftJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join"
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
                .Join(
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
                Assert.IsNull(record);
            }
        }

        [Test]
        public void StreamStreamJoinSpecificSerdes()
        {
            var stringSerdes = new StringSerDes();
            var config = new StreamConfig
            {
                ApplicationId = "test-stream-stream-join"
            };
            config.UseRandomRocksDbConfigForTest();

            StreamBuilder builder = new StreamBuilder();

            var stream = builder.Stream("topic1", stringSerdes, stringSerdes);

            builder
                .Stream("topic2", stringSerdes, stringSerdes)
                .Join(
                    stream,
                    (s, v) => $"{s}-{v}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(5)),
                    StreamJoinProps.With(
                        keySerde: stringSerdes,
                        valueSerde: stringSerdes,
                        otherValueSerde: stringSerdes))
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string, StringSerDes, StringSerDes>("topic1");
                var outputTopic = driver.CreateOuputTopic<string, string, StringSerDes, StringSerDes>("output-join");
                inputTopic.PipeInput("test", "test");
                var record = outputTopic.ReadKeyValue();
                Assert.IsNull(record);
            }

            config.RemoveRocksDbFolderForTest();
        }
    }
}