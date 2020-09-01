using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Public
{
    /// <summary>
    /// Unit test inspired by @jeanlouisboudart
    /// See : https://github.com/jeanlouisboudart/kafka-streams-examples/blob/timestamp-sync-edge-case/src/test/java/io/confluent/examples/streams/StreamTableJoinTimestampSynchronizationIntegrationTest.java
    /// </summary>
    public class StreamTableJoinTimestampSynchronizationIntegrationTests
    {
        internal class MyTimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record,
                long partitionTime)
            {
                string value = record.Message.Value.ToString();
                return Convert.ToInt64(value.Split("|")[0]);
            }
        }

        private static readonly string userClicksTopic = "user-clicks";
        private static readonly string userRegionsTopic = "user-regions";
        private static readonly string outputTopic = "output-topic";
        private static readonly string intermediateTopic = "intermediate-topic";

        #region Helpers

        private StreamConfig BuildStreamConfig()
        {
            StreamConfig<StringSerDes, StringSerDes> config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "stream-table-join-timestamp-sync-test";
            config.PollMs = (long)TimeSpan.FromSeconds(1).TotalMilliseconds;
            return config;
        }

        private string Join(string s1, string s2)
            => $"{s1} --- {s2}";

        private Topology SimpleJoinTopology(ITimestampExtractor timestampExtractor)
        {
            StringSerDes stringSerdes = new StringSerDes();

            var builder = new StreamBuilder();

            // a simple use case with a KStream Topic that needs to join with a KTable
            // internally KTable are versioned per timestamp.
            // the timestamp used by default is ingestion time,
            // but you can change that to your need to use event time (a field from the message) by configuring
            // a timestamp extractor on the stream and / or table
            // when doing a KStreams/KTable join the framework will look up for the value of a given key
            // in the KTable at a timestamp <= to the timestamp of the event on the stream side
            var userRegionsTable = builder.Table<string, string>(userRegionsTopic, stringSerdes, stringSerdes, InMemory<string, string>.As("table-store"), "table", timestampExtractor);
            var userClicksStream = builder.Stream<string, string>(userClicksTopic, stringSerdes, stringSerdes, timestampExtractor);

            userClicksStream
                    .Join(userRegionsTable, Join)
                    .To(outputTopic);

            return builder.Build();
        }

        private Topology KStreamWithImplicitReKeyJoinTopology(ITimestampExtractor timestampExtractor)
        {
            StringSerDes stringSerdes = new StringSerDes();

            var builder = new StreamBuilder();

            var userRegionsTable = builder.Table<string, string>(userRegionsTopic, stringSerdes, stringSerdes, InMemory<string, string>.As("table-store"), "table", timestampExtractor);
            var userClicksStream = builder.Stream<string, string>(userClicksTopic, stringSerdes, stringSerdes, timestampExtractor);

            userClicksStream
                    .SelectKey((k, v) => k)
                    .Join(userRegionsTable, Join)
                    .To(outputTopic);

            return builder.Build();
        }

        private Topology KStreamWithIntermediateTopicReKeyJoinTopology(ITimestampExtractor timestampExtractor)
        {
            StringSerDes stringSerdes = new StringSerDes();

            var builder = new StreamBuilder();

            var userRegionsTable = builder.Table<string, string>(userRegionsTopic, stringSerdes, stringSerdes, InMemory<string, string>.As("table-store"), "table", timestampExtractor);
            var userClicksStream = builder.Stream<string, string>(userClicksTopic, stringSerdes, stringSerdes, timestampExtractor);

            // TODO : update when through processor is DONE
            userClicksStream
                    .SelectKey((k, v) => k)
                    .Join(userRegionsTable, Join)
                    .To(outputTopic);

            return builder.Build();
        }

        private Topology KStreamWithExplicitReKeyJoinWithExplicitSubTopology(ITimestampExtractor timestampExtractor)
        {
            StringSerDes stringSerdes = new StringSerDes();

            var builder = new StreamBuilder();

            var userRegionsTable = builder.Table<string, string>(userRegionsTopic, stringSerdes, stringSerdes, InMemory<string, string>.As("table-store"), "table", timestampExtractor);
            var userClicksStream = builder.Stream<string, string>(userClicksTopic, stringSerdes, stringSerdes, timestampExtractor);

            userClicksStream
                .SelectKey((k, v) => k)
                .To(intermediateTopic);

            builder
                .Stream<string, string>(intermediateTopic, stringSerdes, stringSerdes, timestampExtractor)
                .Join(userRegionsTable, Join)
                .To(outputTopic);

            return builder.Build();
        }

        private void AliceChangesRegionsAfterClickingOnWebsite(TestMultiInputTopic<string, string> topic)
        {
            // publish an event in KTABLE
            // at T0 alice leaves in asia
            topic.PipeInput(userRegionsTopic, "alice", "100|asia");

            // publish multiple events in KSTREAM
            // at T1 alice clicks
            topic.PipeInput(userClicksTopic, "alice", "200|click 1");

            // publish multiple events in KTABLE at T2
            // at T2 leaves in europe
            topic.PipeInput(userRegionsTopic, "alice", "300|europe");

            //Flush data in topic
            topic.Flush();
        }

        #endregion

        [Test]
        public void ShouldMatchIfEventArriveInRightOrder()
        {
            var topo = SimpleJoinTopology(new FailOnInvalidTimestamp());

            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var clickTopic = driver.CreateInputTopic<string, string>(userClicksTopic);
                var regionTopic = driver.CreateInputTopic<string, string>(userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                // publish event in ktable
                regionTopic.PipeInput("alice", "100|france");
                //publish event in kstream
                clickTopic.PipeInput("alice", "200|user 1 click");
                //publish event in ktable
                regionTopic.PipeInput("alice", "300|asia");

                // we should have a result as timestamp from Ktable is <= from the timestamp from the event on KStream side
                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|user 1 click --- 100|france"), items[0]);
            }
        }

        [Test]
        public void ShouldNotMatchIfEventDoesNotArriveInRightOrder()
        {
            var topo = SimpleJoinTopology(new FailOnInvalidTimestamp());

            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var clickTopic = driver.CreateInputTopic<string, string>(userClicksTopic);
                var regionTopic = driver.CreateInputTopic<string, string>(userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                //publish event in kstream
                clickTopic.PipeInput("alice", "200|user 1 click");
                // publish event in ktable
                regionTopic.PipeInput("alice", "100|france");

                // we should have no result as the timestamp from KTable is > to KStream side
                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(0, items.Count);
            }
        }

        [Test]
        public void ShouldMatchIfEventArriveDoesNotInRightOrderWithTimestampExtractor()
        {
            var topo = SimpleJoinTopology(new MyTimestampExtractor());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                //publish event in kstream
                topic.PipeInput(userClicksTopic, "alice", "200|user 1 click");
                // publish event in ktable
                topic.PipeInput(userRegionsTopic, "alice", "100|asia");
                topic.Flush();

                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|user 1 click --- 100|asia"), items[0]);
            }
        }

        [Test]
        public void MultiEvent()
        {
            var topo = SimpleJoinTopology(new MyTimestampExtractor());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                //publish event in kstream
                topic.PipeInputs(userClicksTopic,
                    ("alice", "200|user 1 click"),
                    ("bob", "201|click1"),
                    ("joe", "202|user 1 click"));

                // publish event in ktable
                topic.PipeInputs(userRegionsTopic,
                    ("alice", "100|asia"),
                    ("bob", "101|france"),
                    ("joe", "300|usa"));

                topic.Flush();

                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(2, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|user 1 click --- 100|asia"), items[0]);
                AssertExtensions.MessageEqual(("bob", "201|click1 --- 101|france"), items[1]);
            }
        }

        [Test]
        public void SameTimestampWhenKTableEventIsReceivedAfterKStreamEvent()
        {
            var topo = SimpleJoinTopology(new MyTimestampExtractor());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                // publish event in ktable
                topic.PipeInputs(userRegionsTopic,
                    ("alice", "100|asia"));
                topic.Flush();

                //publish event in kstream
                topic.PipeInputs(userClicksTopic,
                    ("alice", "100|user 1 click"));
                topic.Flush();

                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "100|user 1 click --- 100|asia"), items[0]);
            }
        }

        [Test]
        public void ShouldNotMatchIfEventIsATombstone()
        {
            var topo = SimpleJoinTopology(new FailOnInvalidTimestamp());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                // publish event in ktable
                topic.PipeInputs(userRegionsTopic,
                    ("alice", "100|asia"),
                    ("alice", null));

                //publish event in kstream
                topic.PipeInputs(userClicksTopic,
                    ("alice", "200|click"));
                topic.Flush();

                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(0, items.Count);
            }
        }

        [Test]
        public void ShouldMatchIfTombstoneIsAfterKStreamEvent()
        {
            var topo = SimpleJoinTopology(new FailOnInvalidTimestamp());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                // publish event in ktable
                topic.PipeInputs(userRegionsTopic,
                    ("alice", "100|asia"));

                //publish event in kstream
                topic.PipeInputs(userClicksTopic,
                    ("alice", "200|click"));

                topic.PipeInputs(userRegionsTopic,
                   ("alice", null));

                topic.Flush();

                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|click --- 100|asia"), items[0]);
            }
        }


        /**
        EDGE CASE
        In all cases result should be
        Results should be "200|click 1 --- 100|asia"
        **/

        [Test]
        public void WorksAsExpected()
        {
            var topo = SimpleJoinTopology(new FailOnInvalidTimestamp());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                AliceChangesRegionsAfterClickingOnWebsite(topic);

                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|click 1 --- 100|asia"), items[0]);
            }
        }

        [Test]
        public void ReturnsWrongKTableEventWithRepartitionTopic()
        {
            var topo = KStreamWithImplicitReKeyJoinTopology(new FailOnInvalidTimestamp());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                AliceChangesRegionsAfterClickingOnWebsite(topic);

                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|click 1 --- 100|asia"), items[0]);
            }
        }

        [Test]
        public void ReturnsWrongKTableEventWithIntermediateTopic()
        {
            var topo = KStreamWithIntermediateTopicReKeyJoinTopology(new FailOnInvalidTimestamp());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                AliceChangesRegionsAfterClickingOnWebsite(topic);

                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|click 1 --- 100|asia"), items[0]);
            }
        }

        [Test]
        public void ReturnsWrongKTableEventWithIntermediateTopicAndTimestampExtractor()
        {
            var topo = KStreamWithIntermediateTopicReKeyJoinTopology(new MyTimestampExtractor());
            using (var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);

                AliceChangesRegionsAfterClickingOnWebsite(topic);

                var items = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1);
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|click 1 --- 100|asia"), items[0]);
            }
        }

        [Test]
        public void ReturnsWrongKTableEventWithExplicitSubTopologyAndTimestampExtractor()
        {
            var topo = KStreamWithExplicitReKeyJoinWithExplicitSubTopology(new MyTimestampExtractor());
            var config = BuildStreamConfig();
            config.MaxTaskIdleMs = 1000;
            using (var driver = new TopologyTestDriver(topo, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY))
            {
                var topic = driver.CreateMultiInputTopic<string, string>(userClicksTopic, userRegionsTopic);
                var output = driver.CreateOuputTopic<string, string>(outputTopic);
                AliceChangesRegionsAfterClickingOnWebsite(topic);

                var items = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1);
                Assert.AreEqual(1, items.Count);
                AssertExtensions.MessageEqual(("alice", "200|click 1 --- 100|asia"), items[0]);
            }
        }

    }
}
