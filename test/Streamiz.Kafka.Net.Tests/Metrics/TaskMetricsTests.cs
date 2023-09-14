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
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Metrics
{
    public class TaskMetricsTests
    {
        private StreamMetricsRegistry streamMetricsRegistry = null;

        private readonly StreamConfig<StringSerDes, StringSerDes> config =
            new StreamConfig<StringSerDes, StringSerDes>();

        private SyncKafkaSupplier syncKafkaSupplier;
        private StreamTask task;
        private string threadId = "thread-1";
        private TopicPartition topicPartition;
        private TaskId id;

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
            
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            stream.GroupByKey().Count();
            stream.To("topic2");

            var topo = builder.Build();

            id = new TaskId {Id = 0, Partition = 0};
            var processorTopology = builder.Build().Builder.BuildTopology(id);
            
            syncKafkaSupplier = new SyncKafkaSupplier();
            var producer = syncKafkaSupplier.GetProducer(config.ToProducerConfig());
            var consumer = syncKafkaSupplier.GetConsumer(config.ToConsumerConfig(), null);

            topicPartition = new TopicPartition("topic", 0);
            task = new StreamTask(
                threadId,
                id,
                new List<TopicPartition> {topicPartition},
                processorTopology,
                consumer,
                config,
                syncKafkaSupplier,
                null,
                new MockChangelogRegister(),
                streamMetricsRegistry);
            
            task.InitializeStateStores();
            task.InitializeTopology();
            task.RestorationIfNeeded();
            var activeRestorationSensor = streamMetricsRegistry.GetSensors().FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.ACTIVE_RESTORATION)));
            Assert.AreEqual(1,
                activeRestorationSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.ACTIVE_RESTORATION, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            task.CompleteRestoration();
            Assert.AreEqual(0,
                activeRestorationSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.ACTIVE_RESTORATION, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            
        }
        
        [TearDown]
        public void Dispose()
        {
            task.Suspend();
            task.Close();
            task = null;
        }
        
        [Test]
        public void TaskMetricsTest()
        {
            var serdes = new StringSerDes();
            var cloneConfig = config.Clone();
            cloneConfig.ApplicationId = "consume-test";
            var producer = syncKafkaSupplier.GetProducer(cloneConfig.ToProducerConfig());
            var consumer = syncKafkaSupplier.GetConsumer(cloneConfig.ToConsumerConfig("test-consum"), null);
            consumer.Subscribe("topic2");
            
            int nbMessage = 1000;
            // produce 1000 messages to input topic
            List<ConsumeResult<byte[], byte[]>> messages = new List<ConsumeResult<byte[], byte[]>>();
            int offset = 0;
            for (int i = 0; i < nbMessage; ++i)
                messages.Add(
                    new ConsumeResult<byte[], byte[]>
                    {
                        Message = new Message<byte[], byte[]>
                        {
                            Key = serdes.Serialize($"key{i + 1}", new SerializationContext()),
                            Value = serdes.Serialize($"value{i + 1}", new SerializationContext())
                        },
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, offset++)
                    });

            task.AddRecords(messages);
            
            while (task.CanProcess(DateTime.Now.GetMilliseconds()))
            {
                Assert.IsTrue(task.Process());
                Assert.IsTrue(task.CommitNeeded);
                task.Commit();
            }
            
            var messagesSink = new List<ConsumeResult<byte[], byte[]>>();
            AssertExtensions.WaitUntil(() =>
                {
                    messagesSink.AddRange(consumer.ConsumeRecords(TimeSpan.FromSeconds(1)));
                    return messagesSink.Count < nbMessage;
                }, TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(10));

            long now = DateTime.Now.GetMilliseconds();
            var sensors = streamMetricsRegistry.GetThreadScopeSensor(threadId);

            var processorSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.PROCESS)));
            Assert.AreEqual(2, processorSensor.Metrics.Count());
            Assert.AreEqual(nbMessage,  
                processorSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.PROCESS + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            Assert.IsTrue( 
                (double)(processorSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.PROCESS + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value) > 0d);
            
            var enforcedProcessorSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.ENFORCED_PROCESSING)));
            Assert.AreEqual(2, enforcedProcessorSensor.Metrics.Count());
            Assert.AreEqual(0,  
                enforcedProcessorSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.ENFORCED_PROCESSING + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            Assert.AreEqual(0,
                enforcedProcessorSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.ENFORCED_PROCESSING + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            
            var processLatency = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.PROCESS_LATENCY)));
            Assert.AreEqual(2, processLatency.Metrics.Count());
            Assert.IsTrue(
                (double)processLatency.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.PROCESS_LATENCY + StreamMetricsRegistry.AVG_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)processLatency.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.PROCESS_LATENCY + StreamMetricsRegistry.MAX_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value > 0d);
            
            var commitSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.COMMIT)));
            Assert.AreEqual(2, commitSensor.Metrics.Count());
            Assert.AreEqual(nbMessage,  
                commitSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.COMMIT + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            Assert.IsTrue(
                (double)commitSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.COMMIT + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value > 0d);
            
            var droppedRecordSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.DROPPED_RECORDS)));
            Assert.AreEqual(2, droppedRecordSensor.Metrics.Count());
            Assert.AreEqual(0,  
                droppedRecordSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.DROPPED_RECORDS + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            Assert.AreEqual(0,
                droppedRecordSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.DROPPED_RECORDS + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            
            var activeBufferedRecordSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.ACTIVE_TASK_PREFIX + TaskMetrics.BUFFER_COUNT)));
            Assert.AreEqual(1, activeBufferedRecordSensor.Metrics.Count());
            Assert.AreEqual(0,  
                activeBufferedRecordSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.ACTIVE_TASK_PREFIX + TaskMetrics.BUFFER_COUNT, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);

            var restorationRecordsSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.RESTORATION_RECORDS)));
            Assert.AreEqual(1, restorationRecordsSensor.Metrics.Count());
            Assert.AreEqual(0,  
                restorationRecordsSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.RESTORATION_RECORDS, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
            
            var activeRestorationSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(TaskMetrics.ACTIVE_RESTORATION)));
            Assert.AreEqual(1, activeRestorationSensor.Metrics.Count());
            Assert.AreEqual(0,
                activeRestorationSensor.Metrics[MetricName.NameAndGroup(
                    TaskMetrics.ACTIVE_RESTORATION, 
                    StreamMetricsRegistry.TASK_LEVEL_GROUP)].Value);
        }

        private string GetSensorName(string sensorName)
            => streamMetricsRegistry.FullSensorName(sensorName, streamMetricsRegistry.TaskSensorPrefix(threadId, id.ToString()));

    
    }
}