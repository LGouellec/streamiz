using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamKStreamWindowedJoinTests
    {
        [Test]
        public void StreamStreamLeftJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-windowed-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("windowed-join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(2)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("windowed-join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(2)));

            var stream2 = builder.Stream<string, string>("topic2");


            builder.Stream<string, string>("topic1")
                .LeftJoin<string, string>(
                    stream2,
                    (s, v) => $"{s}-{v}",
                    new JoinSlidingWindowOptions(0L, 2000L, -1L, JoinSlidingWindowOptions.DEFAULT_RETENTION_MS),
                    props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");

                DateTime dateTime = DateTime.Today;
                inputTopic1.PipeInput("test", "left-0", dateTime.AddSeconds(0));
                inputTopic2.PipeInput("test", "right-2", dateTime.AddSeconds(2));
                inputTopic2.PipeInput("test", "right-4", dateTime.AddSeconds(4));

                inputTopic1.PipeInput("test", "left-5", dateTime.AddSeconds(5));
                inputTopic2.PipeInput("test", "right-6", dateTime.AddSeconds(6));
                inputTopic2.PipeInput("test", "right-8", dateTime.AddSeconds(8));


                var records = outputTopic.ReadValueList().ToArray();
                Assert.AreEqual(4, records.Length);

                Assert.That(records[0], Is.EqualTo("left-0-"));
                Assert.That(records[1], Is.EqualTo("left-0-right-2"));
                Assert.That(records[2], Is.EqualTo("left-5-"));
                Assert.That(records[3], Is.EqualTo("left-5-right-6"));
            }
        }

        [Test]
        public void StreamStreamInnerJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-windowed-inner-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("windowed-join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(2)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("windowed-join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(2)));

            var stream2 = builder.Stream<string, string>("topic2");


            builder.Stream<string, string>("topic1")
                .Join<string, string>(
                    stream2,
                    (s, v) => $"{s}-{v}",
                    new JoinSlidingWindowOptions(0L, 2000L, -1L, JoinSlidingWindowOptions.DEFAULT_RETENTION_MS),
                    props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");

                DateTime dateTime = DateTime.Today;
                inputTopic1.PipeInput("test", "left-0", dateTime.AddSeconds(0));
                inputTopic2.PipeInput("test", "right-2", dateTime.AddSeconds(2));
                inputTopic2.PipeInput("test", "right-4", dateTime.AddSeconds(4));

                inputTopic1.PipeInput("test", "left-5", dateTime.AddSeconds(5));
                inputTopic2.PipeInput("test", "right-6", dateTime.AddSeconds(6));
                inputTopic2.PipeInput("test", "right-8", dateTime.AddSeconds(8));

                var records = outputTopic.ReadValueList().ToArray();
                Assert.AreEqual(2, records.Length);

                Assert.That(records[0], Is.EqualTo("left-0-right-2"));
                Assert.That(records[1], Is.EqualTo("left-5-right-6"));
            }
        }

        [Test]
        public void StreamStreamFullOuterJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-windowed-full-outer-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var props = StreamJoinProps.With<string, string, string>(
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("windowed-join-1-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(2)),
                Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("windowed-join-2-store", TimeSpan.FromDays(1),
                    TimeSpan.FromSeconds(2)));

            var stream2 = builder.Stream<string, string>("topic2");


            builder.Stream<string, string>("topic1")
                .OuterJoin<string, string>(
                    stream2,
                    (s, v) => $"{s}-{v}",
                    new JoinSlidingWindowOptions(0L, 2000L, -1L, JoinSlidingWindowOptions.DEFAULT_RETENTION_MS),
                    props)
                .To("output-join");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output-join");

                DateTime dateTime = DateTime.Today;
                inputTopic1.PipeInput("test", "left-0", dateTime.AddSeconds(0) );
                inputTopic2.PipeInput("test", "right-2", dateTime.AddSeconds(2));
                inputTopic2.PipeInput("test", "right-4", dateTime.AddSeconds(4));

                inputTopic1.PipeInput("test", "left-5", dateTime.AddSeconds(5) );
                inputTopic2.PipeInput("test", "right-6", dateTime.AddSeconds(6));
                inputTopic2.PipeInput("test", "right-8", dateTime.AddSeconds(8));


                var records = outputTopic.ReadValueList().ToArray();
                Assert.AreEqual(6, records.Length);

                Assert.That(records[0], Is.EqualTo("left-0-"));
                Assert.That(records[1], Is.EqualTo("left-0-right-2"));
                Assert.That(records[2], Is.EqualTo("-right-4"));
                Assert.That(records[3], Is.EqualTo("left-5-"));
                Assert.That(records[4], Is.EqualTo("left-5-right-6"));
                Assert.That(records[5], Is.EqualTo("-right-8"));
            }
        }


        class JoinSlidingWindowOptions : JoinWindowOptions
        {
            public JoinSlidingWindowOptions(long beforeMs, long afterMs, long graceMs, long maintainDurationMs)
                : base(beforeMs, afterMs, graceMs, maintainDurationMs)
            {
            }
        }

    }
}