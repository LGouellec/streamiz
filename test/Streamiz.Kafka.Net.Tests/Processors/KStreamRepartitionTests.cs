using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamRepartitionTests
    {
        private IEnumerable<ConsumeResult<string, string>> SetupTest(Action<StreamBuilder> builderFunction, TopologyTestDriver.Mode mode = TopologyTestDriver.Mode.SYNC_TASK, int numberPartition = 1)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-repartition-processor"
            };
            
            StreamBuilder builder = new StreamBuilder();
            builderFunction(builder);

            Topology t = builder.Build();

            IKafkaSupplier supplier = mode == TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY
                ? new MockKafkaSupplier(numberPartition)
                : null;

            using (var driver = new TopologyTestDriver(t.Builder, config, mode, supplier))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "test1");
                inputTopic.PipeInput("test", "test2");
                inputTopic.PipeInput("test", "test3");
                var records = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(outputTopic, 3);
                var recordsMap = records.ToUpdateDictionary(r => r.Message.Key, r => r.Message.Value);
                Assert.IsNotNull(recordsMap);
                Assert.AreEqual(1, recordsMap.Count);
                Assert.AreEqual("test3", recordsMap["test"]);
                return records;
            }
        }
        
        [Test]
        public void RepartitionCreateWithSerDes()
        {
           SetupTest((builder) =>
           {
               builder
                   .Stream<string, string>("topic")
                   .Repartition(Repartitioned<string, string>.Create<StringSerDes, StringSerDes>())
                   .To("output");
           });
        }
        
        [Test]
        public void RepartitionWithExplicitSerDes()
        {
            SetupTest((builder) =>
            {
                builder
                    .Stream<string, string>("topic")
                    .Repartition(Repartitioned<string, string>.With(new StringSerDes(), new StringSerDes()))
                    .To("output");
            });
        }
        
        [Test]
        public void RepartitionAs()
        {
            SetupTest((builder) =>
            {
                builder
                    .Stream<string, string>("topic")
                    .Repartition(Repartitioned<string, string>.As("repartition-processor"))
                    .To("output");
            });
        }
        
        [Test]
        public void RepartitionWithPartitioner()
        {
            var records = SetupTest((builder) =>
            {
                builder
                    .Stream<string, string>("topic")
                    .Repartition(Repartitioned<string, string>.Empty().WithStreamPartitioner((t,k,v,_, c) => 0))
                    .To("output");
            }, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, 10);

            Assert.AreEqual(0, records.ToList()[0].Partition.Value);
            Assert.AreEqual(0, records.ToList()[1].Partition.Value);
            Assert.AreEqual(0, records.ToList()[2].Partition.Value);
        }
        
        [Test]
        public void RepartitionWithPartitionNumber()
        {
            var records = SetupTest((builder) =>
            {
                builder
                    .Stream<string, string>("topic")
                    .Repartition(Repartitioned<string, string>.Empty().WithNumberOfPartitions(20))
                    .To("output");
            }, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);

            // topic - 1 partitions
            // repartitionTopic - 20 partitions
            // output - 1 partitions
            
            Assert.AreEqual(0, records.ToList()[0].Partition.Value);
            Assert.AreEqual(0, records.ToList()[1].Partition.Value);
            Assert.AreEqual(0, records.ToList()[2].Partition.Value);
        }
    
    }
}