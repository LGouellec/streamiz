using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Metrics
{
    public class ProcessorNodeMetricsTests
    {
        private StreamMetricsRegistry streamMetricsRegistry = null;

        private readonly StreamConfig<StringSerDes, StringSerDes> config =
            new StreamConfig<StringSerDes, StringSerDes>();

        private string threadId = StreamMetricsRegistry.UNKNOWN_THREAD;
        private string processorNodeName = "source-processor";
        private TopicPartition topicPartition;
        private TaskId id;
        private SourceProcessor<string, string> sourceProcessor;
        private ProcessorContext context;

        [SetUp]
        public void Initialize()
        {
            streamMetricsRegistry
                = new StreamMetricsRegistry(Guid.NewGuid().ToString(),
                    MetricsRecordingLevel.DEBUG);
            
            config.ApplicationId = "test-stream-thread";
            config.StateDir = Guid.NewGuid().ToString();
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.CommitIntervalMs = 1;
            
            
            id = new TaskId {Id = 0, Partition = 0};
            topicPartition = new TopicPartition("topic", 0);

            sourceProcessor = new SourceProcessor<string, string>(
                processorNodeName,
                "topic",
                new StringSerDes(),
                new StringSerDes(),
                new FailOnInvalidTimestamp());
            sourceProcessor.AddNextProcessor(new KStreamPeekProcessor<string, string>((k, v) => { }, false));

            context = new ProcessorContext(
                UnassignedStreamTask.Create(id),
                config,
                new ProcessorStateManager(
                    id,
                    new List<TopicPartition> {topicPartition},
                    null,
                    new StoreChangelogReader(config, null, threadId, streamMetricsRegistry),
                    new MockOffsetCheckpointManager()
                ), streamMetricsRegistry);
            
            sourceProcessor.Init(context);
        }
        
        [TearDown]
        public void Dispose()
        {
        }
        
        [Test]
        public void ProcessorNodeMetricsTest()
        {
            var serdes = new StringSerDes();
            var rd = new Random();
            int nbMessage = rd.Next(0, 4000);
            
            // simulate processing message
            for (int i = 0; i < nbMessage; ++i)
            {
                string key = "key" + i, value = "value" + i;
                var r = new ConsumeResult<byte[], byte[]>()
                {
                    Message = new Message<byte[], byte[]>()
                    {
                        Key = serdes.Serialize(key, new SerializationContext()),
                        Value = serdes.Serialize(value, new SerializationContext()),
                        Timestamp = new Timestamp(DateTime.Now)
                    },
                    Offset = i,
                    IsPartitionEOF = false,
                    Topic = topicPartition.Topic,
                    Partition = topicPartition.Partition
                };
                
                context.SetRecordMetaData(r);
                sourceProcessor.Process(key, value);
            }

            long now = DateTime.Now.GetMilliseconds();
            var sensors = streamMetricsRegistry.GetThreadScopeSensor(threadId);
            foreach (var s in sensors)
                s.Refresh(now);
            
            var processorSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ProcessorNodeMetrics.PROCESS)));
            Assert.AreEqual(2, processorSensor.Metrics.Count());
            Assert.AreEqual(nbMessage,  
                processorSensor.Metrics[MetricName.NameAndGroup(
                    ProcessorNodeMetrics.PROCESS + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.PROCESSOR_NODE_LEVEL_GROUP)].Value);
            Assert.IsTrue(
                (double)processorSensor.Metrics[MetricName.NameAndGroup(
                    ProcessorNodeMetrics.PROCESS + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.PROCESSOR_NODE_LEVEL_GROUP)].Value > 0d);
        }

        private string GetSensorName(string sensorName)
            => streamMetricsRegistry.FullSensorName(sensorName, streamMetricsRegistry.NodeSensorPrefix(threadId, id.ToString(), processorNodeName));

    
    }
}