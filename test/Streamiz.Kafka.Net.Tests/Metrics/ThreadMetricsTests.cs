using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Metrics
{
    public class ThreadMetricsTests
    {
        class ThreadMetricProcessor : Net.Processors.Public.IProcessor<string, string>
        {
            public void Init(ProcessorContext<string, string> context)
            {
                context.Schedule(
                    TimeSpan.FromMilliseconds(25),
                    PunctuationType.PROCESSING_TIME,
                    (now) =>
                    {
                        Thread.Sleep(5);
                    });
            }

            public void Process(Record<string, string> record)
            {
                
            }

            public void Close()
            {
                
            }
        }
        
        private StreamMetricsRegistry streamMetricsRegistry = null;
        private readonly CancellationTokenSource token = new System.Threading.CancellationTokenSource();

        private readonly StreamConfig<StringSerDes, StringSerDes> config =
            new StreamConfig<StringSerDes, StringSerDes>();

        private MockKafkaSupplier mockKafkaSupplier;
        private StreamThread thread;
        private string threadId = "thread-1";
        private int numberPartitions = 4;

        [SetUp]
        public void Initialize()
        {
            streamMetricsRegistry
                = new StreamMetricsRegistry(Guid.NewGuid().ToString(),
                    MetricsRecordingLevel.INFO);
            
            config.ApplicationId = "test-stream-thread";
            config.StateDir = Guid.NewGuid().ToString();
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.MaxPollRecords = 10;
            config.CommitIntervalMs = 10;
            config.MetricsRecording = MetricsRecordingLevel.INFO;

            mockKafkaSupplier = new MockKafkaSupplier(numberPartitions);

            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            stream.Process(
                new ProcessorBuilder<string, string>()
                    .Processor<ThreadMetricProcessor>()
                    .Build());
            stream.To("topic2");

            var topo = builder.Build();

            thread = StreamThread.Create(
                threadId, "c1",
                topo.Builder, streamMetricsRegistry, config,
                mockKafkaSupplier, mockKafkaSupplier.GetAdmin(config.ToAdminConfig("admin")),
                1) as StreamThread;
        }
        
        [TearDown]
        public void Dispose()
        {
            token.Cancel();
            thread.Dispose();
            mockKafkaSupplier.Destroy();
        }
        
        [Test]
        public void ThreadMetricsTest()
        {
            var serdes = new StringSerDes();
            var cloneConfig = config.Clone();
            cloneConfig.ApplicationId = "consume-test";
            var producer = mockKafkaSupplier.GetProducer(cloneConfig.ToProducerConfig());
            var consumer = mockKafkaSupplier.GetConsumer(cloneConfig.ToConsumerConfig("test-consum"), null);
            consumer.Subscribe("topic2");
            
            thread.Start(token.Token);
            
            AssertExtensions.WaitUntil(() => thread.ActiveTasks.Count() == numberPartitions,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(100));

            int nbMessage = 1000;
            // produce ${nbMessage} messages to input topic
            for (int i = 0; i < nbMessage; ++i)
            {
                producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
                {
                    Key = serdes.Serialize("key" + i, new SerializationContext()),
                    Value = serdes.Serialize("Hi" + i, new SerializationContext())
                });
            }
            
            var messagesSink = new List<ConsumeResult<byte[], byte[]>>();
            
            AssertExtensions.WaitUntil(() =>
                {
                    messagesSink.AddRange(consumer.ConsumeRecords(TimeSpan.FromSeconds(1)));
                    return messagesSink.Count == nbMessage;
                }, TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(10));
            
            // waiting end of processing
            Thread.Sleep(1000);
            
            long now = DateTime.Now.GetMilliseconds();
            var sensors = streamMetricsRegistry.GetThreadScopeSensor(threadId);

            var createTaskSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.CREATE_TASK)));
            Assert.AreEqual(2, createTaskSensor.Metrics.Count());
            Assert.AreEqual(numberPartitions, 
                createTaskSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.CREATE_TASK + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value);
            Assert.IsTrue( 
                (double)(createTaskSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.CREATE_TASK + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value) > 0d);
            
            var closeTaskSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.CLOSE_TASK)));
            Assert.AreEqual(2, closeTaskSensor.Metrics.Count());
            Assert.AreEqual(0d, 
                closeTaskSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.CLOSE_TASK + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value);
            Assert.AreEqual(0d, 
                closeTaskSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.CLOSE_TASK + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value);
            
            var commitSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.COMMIT)));
            Assert.AreEqual(4, commitSensor.Metrics.Count());
            Assert.IsTrue( 
                (double)commitSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.COMMIT + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)commitSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.COMMIT + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)commitSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.COMMIT + StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.AVG_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)commitSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.COMMIT + StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.MAX_SUFFIX,
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            
            var pollSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.POLL)));
            Assert.AreEqual(4, pollSensor.Metrics.Count());
            Assert.IsTrue( 
                (double)pollSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.POLL + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)pollSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.POLL + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)pollSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.POLL + StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.AVG_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)pollSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.POLL + StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.MAX_SUFFIX,
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            
            var pollRecordsSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.POLL + StreamMetricsRegistry.RECORDS_SUFFIX)));
            Assert.AreEqual(2, pollRecordsSensor.Metrics.Count());
            Assert.IsTrue(
                (double)pollRecordsSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.POLL + StreamMetricsRegistry.RECORDS_SUFFIX + StreamMetricsRegistry.AVG_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)pollRecordsSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.POLL + StreamMetricsRegistry.RECORDS_SUFFIX + StreamMetricsRegistry.MAX_SUFFIX,
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            
            var processRecordsSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.PROCESS + StreamMetricsRegistry.RECORDS_SUFFIX)));
            Assert.AreEqual(2, processRecordsSensor.Metrics.Count());
            Assert.IsTrue(
                (double)processRecordsSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PROCESS + StreamMetricsRegistry.RECORDS_SUFFIX + StreamMetricsRegistry.AVG_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)processRecordsSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PROCESS + StreamMetricsRegistry.RECORDS_SUFFIX + StreamMetricsRegistry.MAX_SUFFIX,
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            
            var processRateSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.PROCESS + StreamMetricsRegistry.RATE_SUFFIX)));
            Assert.AreEqual(2, processRateSensor.Metrics.Count());
            Assert.IsTrue(
                (double)processRateSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PROCESS + StreamMetricsRegistry.RATE_SUFFIX + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)processRateSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PROCESS + StreamMetricsRegistry.RATE_SUFFIX + StreamMetricsRegistry.TOTAL_SUFFIX,
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            
            var processLatencySensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.PROCESS + StreamMetricsRegistry.LATENCY_SUFFIX)));
            Assert.AreEqual(2, processLatencySensor.Metrics.Count());
            Assert.IsTrue(
                (double)processLatencySensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PROCESS + StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.AVG_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)processLatencySensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PROCESS + StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.MAX_SUFFIX,
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            
            // Punctuate sensor
            var punctuateSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.PUNCTUATE)));
            Assert.AreEqual(4, punctuateSensor.Metrics.Count());
            Assert.IsTrue( 
                (double)punctuateSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PUNCTUATE + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)punctuateSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PUNCTUATE + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)punctuateSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PUNCTUATE + StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.AVG_SUFFIX, 
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            Assert.IsTrue(
                (double)punctuateSensor.Metrics[MetricName.NameAndGroup(
                    ThreadMetrics.PUNCTUATE + StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.MAX_SUFFIX,
                    StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value > 0d);
            
            // ratio sensors
            var processRatioSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.PROCESS + StreamMetricsRegistry.RATIO_SUFFIX)));
            var pollRatioSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.POLL + StreamMetricsRegistry.RATIO_SUFFIX)));
            var commitRatioSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.COMMIT + StreamMetricsRegistry.RATIO_SUFFIX)));
            var punctuateRatioSensor = sensors.FirstOrDefault(s => s.Name.Equals(GetSensorName(ThreadMetrics.PUNCTUATE + StreamMetricsRegistry.RATIO_SUFFIX)));
            
            Assert.AreEqual(1, processRatioSensor.Metrics.Count());
            Assert.AreEqual(1, pollRatioSensor.Metrics.Count());
            Assert.AreEqual(1, commitRatioSensor.Metrics.Count());
            Assert.AreEqual(1, punctuateRatioSensor.Metrics.Count());

            var processRatioValue = (double) processRatioSensor.Metrics[MetricName.NameAndGroup(
                ThreadMetrics.PROCESS + StreamMetricsRegistry.RATIO_SUFFIX,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value;

            var pollRatioValue = (double) pollRatioSensor.Metrics[MetricName.NameAndGroup(
                ThreadMetrics.POLL + StreamMetricsRegistry.RATIO_SUFFIX,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value;

            var commitRatioValue = (double) commitRatioSensor.Metrics[MetricName.NameAndGroup(
                ThreadMetrics.COMMIT + StreamMetricsRegistry.RATIO_SUFFIX,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value;
            
            var punctuateRatioValue = (double) punctuateRatioSensor.Metrics[MetricName.NameAndGroup(
                ThreadMetrics.PUNCTUATE + StreamMetricsRegistry.RATIO_SUFFIX,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP)].Value;


            double total = Math.Round(processRatioValue + pollRatioValue + commitRatioValue + punctuateRatioValue, 2);
            // we accept 10% of lost
            Assert.IsTrue(total >= 0.90d);
        }

        private string GetSensorName(string sensorName)
            => streamMetricsRegistry.FullSensorName(sensorName, streamMetricsRegistry.ThreadSensorPrefix(threadId));

    }
}