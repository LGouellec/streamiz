using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Private
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

        private StreamConfig BuildStreamConfig()
        {
            StreamConfig<StringSerDes, StringSerDes> config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "stream-table-join-timestamp-sync-test";
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
    
        [Test]
        public void ShouldMatchIfEventArriveInRightOrder()
        {
            var topo = SimpleJoinTopology(new FailOnInvalidTimestamp());

            using(var driver = new TopologyTestDriver(topo, BuildStreamConfig()))
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
                Assert.AreEqual("alice", items[0].Message.Key);
                Assert.AreEqual("200|user 1 click --- 100|france", items[0].Message.Value);
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
            // InternalTopologyBuilder line 672
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

                // we should have no result as the timestamp from KTable is > to KStream side
                var items = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, items.Count);
                Assert.AreEqual("alice", items[0].Message.Key);
                Assert.AreEqual("200|user 1 click --- 100|asia", items[0].Message.Value);
            }
        }
    }
}
