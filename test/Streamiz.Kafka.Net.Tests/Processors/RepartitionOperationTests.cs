using System;
using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class RepartitionOperationTests
    {
        [Test]
        public void RepartitionTestTopology1()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-repartition-processor"
            };
            
            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v))
                .GroupByKey()
                .Count()
                .ToStream()
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>("output");
                inputTopic.PipeInput("test", "test1");
                inputTopic.PipeInput("test", "test2");
                inputTopic.PipeInput("test", "test3");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual(3, records["TEST"]);
            }
        }

        [Test]
        public void RepartitionTestTopologyAsync1()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-repartition-processor"
            };
            
            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v))
                .GroupByKey()
                .Count()
                .ToStream()
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>("output");
                inputTopic.PipeInput("test", "test1");
                inputTopic.PipeInput("test", "test2");
                inputTopic.PipeInput("test", "test3");
                var records = IntegrationTestUtils
                    .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
                    .ToUpdateDictionary(r => r.Message.Key, r => r.Message.Value);
                Assert.IsNotNull(records);
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual(3, records["TEST"]);
            }
        }

        [Test]
        public void RepartitionTestJoinTopology()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-repartition-processor"
            };

            var supplier = new MockKafkaSupplier(10);
            
            StreamBuilder builder = new StreamBuilder();

            IKStream<string, string> stream1 = builder
                .Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v));
            
            IKStream<string, string> stream2 = builder.Stream<string, string>("topic2");

            stream1.Join(stream2,
                    (v1, v2) => $"{v1}-{v2}",
                    JoinWindowOptions.Of(TimeSpan.FromMinutes(1)),
                    StreamJoinProps.As<string, string, string>("join-store"))
                .To("output");
                
            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "coucou");
                inputTopic2.PipeInput("TEST", "sylvain");
                inputTopic2.PipeInput("TEST2", "antoine");
                inputTopic.PipeInput("test2", "test");
                var records = IntegrationTestUtils
                    .WaitUntilMinKeyValueRecordsReceived(outputTopic, 2)
                    .ToUpdateDictionary(r => r.Message.Key, r => r.Message.Value);
                Assert.IsNotNull(records);
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("coucou-sylvain", records["TEST"]);
                Assert.AreEqual("test-antoine", records["TEST2"]);
            }
        }
        
        [Test]
        public void RepartitionTestTopologyAsyncHighNumberPartititon()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-repartition-processor"
            };

            var supplier = new MockKafkaSupplier(30);
            
            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v))
                .GroupByKey()
                .Count()
                .ToStream()
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>("output");
                inputTopic.PipeInput("test", "test1");
                inputTopic.PipeInput("test", "test2");
                inputTopic.PipeInput("test", "test3");
                inputTopic.PipeInput("test", "test4");
                inputTopic.PipeInput("test", "test5");
                var records = IntegrationTestUtils
                    .WaitUntilMinKeyValueRecordsReceived(outputTopic, 5)
                    .ToUpdateDictionary(r => r.Message.Key, r => r.Message.Value);
                Assert.IsNotNull(records);
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual(5, records["TEST"]);
            }

        }
    }
}