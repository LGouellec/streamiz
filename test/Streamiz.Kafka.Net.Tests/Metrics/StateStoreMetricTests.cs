using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Metered;

namespace Streamiz.Kafka.Net.Tests.Metrics
{
    public class StateStoreMetricTests
    {
        /// <summary>
        /// Just mock to add some ms latency to measure it
        /// </summary>
        private class MockInMemoryStore : InMemoryKeyValueStore
        {
            private bool putAll = false;
            
            private void Wait()
                => Wait(1, 100);
            
            private void Wait(int b, int e)
            {
                Random rd = new Random();
                Thread.Sleep(rd.Next(b, e));
            }
            
            public MockInMemoryStore(string name) : base(name)
            {
            }

            public override void Flush()
            {
                Wait();
                base.Flush();
            }

            public override byte[] Delete(Bytes key)
            {
                Wait();
                return base.Delete(key);
            }

            public override byte[] Get(Bytes key)
            {
                Wait(1, 5);
                return base.Get(key);
            }

            public override IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            {
                Wait();
                return base.All();
            }

            public override IKeyValueEnumerator<Bytes, byte[]> Range(Bytes @from, Bytes to)
            {
                Wait();
                return base.Range(@from, to);
            }

            public override void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
            {
                Wait();
                putAll = true;
                base.PutAll(entries);
                putAll = false;
            }

            public override void Put(Bytes key, byte[] value)
            {
                if(!putAll)
                    Wait(10, 20);
                base.Put(key, value);
            }

            public override byte[] PutIfAbsent(Bytes key, byte[] value)
            {
                Wait(1, 5);
                return base.PutIfAbsent(key, value);
            }
        }

        /// <summary>
        /// Just mock to add some ms latency to measure it
        /// </summary>
        private class MockInMemoryWindowStore : InMemoryWindowStore
        {
            private void Wait()
                => Wait(1, 100);
            
            private void Wait(int b, int e)
            {
                Random rd = new Random();
                Thread.Sleep(rd.Next(b, e));
            }
            
            public MockInMemoryWindowStore(string storeName, TimeSpan retention, long size) 
                : base(storeName, retention, size, false)
            {
            }

            public override IKeyValueEnumerator<Windowed<Bytes>, byte[]> All()
            {
                Wait();
                return base.All();
            }

            public override IWindowStoreEnumerator<byte[]> Fetch(Bytes key, DateTime @from, DateTime to)
            {
                Wait();
                return base.Fetch(key, @from, to);
            }

            public override IWindowStoreEnumerator<byte[]> Fetch(Bytes key, long @from, long to)
            {
                Wait();
                return base.Fetch(key, @from, to);
            }

            public override byte[] Fetch(Bytes key, long time)
            {
                Wait(1, 5);
                return base.Fetch(key, time);
            }

            public override void Flush()
            {
                Wait();
                base.Flush();
            }

            public override void Put(Bytes key, byte[] value, long windowStartTimestamp)
            {
                Wait(1, 5);
                base.Put(key, value, windowStartTimestamp);
            }

            public override IKeyValueEnumerator<Windowed<Bytes>, byte[]> FetchAll(DateTime @from, DateTime to)
            {
                Wait();
                return base.FetchAll(@from, to);
            }
        }
        
        private StreamMetricsRegistry streamMetricsRegistry = null;

        private readonly StreamConfig<StringSerDes, StringSerDes> config =
            new StreamConfig<StringSerDes, StringSerDes>();

        private string threadId = StreamMetricsRegistry.UNKNOWN_THREAD;
        private TopicPartition topicPartition;
        private TaskId id;
        private string storeScope = "store-scope";
        private string storeName = "my-store";
        private ProcessorContext context;
        private IStateStore store;
        
        [SetUp]
        public void Initialize()
        {
            threadId = Thread.CurrentThread.Name ?? StreamMetricsRegistry.UNKNOWN_THREAD;

            streamMetricsRegistry
                = new StreamMetricsRegistry(Guid.NewGuid().ToString(),
                    MetricsRecordingLevel.DEBUG);
            
            config.ApplicationId = "test-stream-thread";
            config.StateDir = Guid.NewGuid().ToString();
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.CommitIntervalMs = 1;

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            id = new TaskId {Id = 0, Partition = 0};
            topicPartition = new TopicPartition("topic", 0);
            
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
        }
        
        [TearDown]
        public void Dispose()
        {
            store.Close();
        }
        
        [Test]
        public void KeyValueStoreMetricsTest()
        {
            var random = new Random();
            MeteredKeyValueStore<string, string> meteredKeyValueStore = new MeteredKeyValueStore<string, string>(
                new MockInMemoryStore(storeName),
                new StringSerDes(),
                new StringSerDes(),
                storeScope);
            store = meteredKeyValueStore;
            meteredKeyValueStore.Init(context, meteredKeyValueStore);
            
            
            int nbMessage = random.Next(0, 30000);
            int nbMessage2 = random.Next(0, 30);
            // produce ${nbMessage} messages to input topic
            List<KeyValuePair<string, string>> messages = new List<KeyValuePair<string, string>>();
            for (int i = 0; i < nbMessage; ++i)
                messages.Add(new KeyValuePair<string, string>($"key{i + 1}", $"value{i + 1}"));

            meteredKeyValueStore.PutAll(messages);
            meteredKeyValueStore.Flush();
            
           // AssertAvgAndMaxLatency(StateStoreMetrics.PUT_ALL);
            var latencyAvg = GetSensorMetric(
                StateStoreMetrics.PUT_ALL,
                StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.AVG_SUFFIX,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP);
            var latencyMax = GetSensorMetric(
                StateStoreMetrics.PUT_ALL,
                StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.MAX_SUFFIX,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP);
            
            Assert.IsTrue((double)latencyAvg.Value > 0);
            Assert.IsTrue((double)latencyMax.Value > 0);
            
            for(int i = 0 ; i < nbMessage2 ; ++i)
                meteredKeyValueStore.Put($"test{i}", $"test{i}");
            meteredKeyValueStore.Flush();
            
            
            AssertAvgAndMaxLatency(StateStoreMetrics.PUT);
            
            for(int i = 0 ; i < nbMessage2 ; ++i)
                meteredKeyValueStore.PutIfAbsent($"test{i}", $"test{i}");
            meteredKeyValueStore.Flush();
            
            AssertAvgAndMaxLatency(StateStoreMetrics.PUT_IF_ABSENT);

            for (int i = 0; i < nbMessage2; ++i)
                meteredKeyValueStore.Get($"test{i}");
            
            AssertAvgAndMaxLatency(StateStoreMetrics.GET);

            for(int i = 0 ; i < 5 ; ++i)
                meteredKeyValueStore.All();
            
            AssertAvgAndMaxLatency(StateStoreMetrics.ALL);

            var results1 = meteredKeyValueStore.Range($"key0", $"key{nbMessage - 1}").ToList();
            var results2 = meteredKeyValueStore.Range($"test0", $"test{nbMessage2 - 1}").ToList();
            
            AssertAvgAndMaxLatency(StateStoreMetrics.RANGE);

            for (int i = 0; i < nbMessage2; ++i)
                meteredKeyValueStore.Delete($"key{i + 1}");
            
            AssertAvgAndMaxLatency(StateStoreMetrics.DELETE);
            AssertAvgAndMaxLatency(StateStoreMetrics.FLUSH);
        }

        [Test]
        public void WindowStoreMetricsTest()
        {
            long windowSize = 1000 * 60;
            var random = new Random();
            MeteredWindowStore<string, string> meteredWindowStore = new MeteredWindowStore<string, string>(
                new MockInMemoryWindowStore(storeName, TimeSpan.FromDays(1), windowSize),
                windowSize,
                new StringSerDes(),
                new StringSerDes(),
                storeScope);
            store = meteredWindowStore;
            meteredWindowStore.Init(context, meteredWindowStore);
            
            int nbMessage = random.Next(0, 30);
            long now1 = DateTime.Now.GetMilliseconds();
            
            // produce ${nbMessage} messages to input topic (both);
            for(int i = 0 ; i < nbMessage ; ++i)
                meteredWindowStore.Put($"test{i}", $"test{i}", now1);
            
            meteredWindowStore.Flush();
            
            long now2 = DateTime.Now.GetMilliseconds();
            for(int i = 0 ; i < nbMessage ; ++i)
                meteredWindowStore.Put($"test{i}", $"test{i}", now2);
            
            meteredWindowStore.Flush();
            
            AssertAvgAndMaxLatency(StateStoreMetrics.PUT);
            AssertAvgAndMaxLatency(StateStoreMetrics.FLUSH);
        
            for (int i = 0; i < nbMessage; ++i)
                meteredWindowStore.Fetch($"test{i}", now1);
            
            AssertAvgAndMaxLatency(StateStoreMetrics.FETCH);

            meteredWindowStore.Fetch($"test0", now1.FromMilliseconds().AddSeconds(-10),
                now1.FromMilliseconds().AddSeconds(10)).ToList();
            
            AssertAvgAndMaxLatency(StateStoreMetrics.FETCH);

            meteredWindowStore.Fetch($"test0", now1 - 10000,
                now1 + 10000).ToList();
            
            AssertAvgAndMaxLatency(StateStoreMetrics.FETCH);

            var nb = meteredWindowStore.FetchAll(now1.FromMilliseconds().AddSeconds(-10),
                now2.FromMilliseconds().AddSeconds(10)).ToList().Count();
            
            Assert.AreEqual(nbMessage * 2 , nb);
            
            AssertAvgAndMaxLatency(StateStoreMetrics.FETCH);

            meteredWindowStore.All().ToList();
        }
        
        private void AssertAvgAndMaxLatency(string sensorName)
        {
            var latencyAvg = GetSensorMetric(
                sensorName,
                StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.AVG_SUFFIX,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP);
            var latencyMax = GetSensorMetric(
                sensorName,
                StreamMetricsRegistry.LATENCY_SUFFIX + StreamMetricsRegistry.MAX_SUFFIX,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP);
            Assert.IsTrue((double)latencyAvg.Value > 0);
            Assert.IsTrue((double)latencyMax.Value > 0);
        }
        
        private StreamMetric GetSensorMetric(string sensorName, string metricSuffix, string group)
        {
            long now = DateTime.Now.GetMilliseconds();
            var sensor = streamMetricsRegistry.GetSensors().FirstOrDefault(s => s.Name.Equals(GetSensorName(sensorName)));
            if (sensor == null)
                throw new NullReferenceException($"sensor {sensorName} not found");

            MetricName keyMetric = MetricName.NameAndGroup(
                sensorName + metricSuffix,
                group);
            
            if (!sensor.Metrics.ContainsKey(keyMetric))
                throw new NullReferenceException($"metric {sensorName + metricSuffix}|{group} not found inside {sensorName}");
            
            return sensor.Metrics[keyMetric];
        }
        
        private string GetSensorName(string sensorName)
            => streamMetricsRegistry.FullSensorName(
                sensorName,
                streamMetricsRegistry.StoreSensorPrefix(threadId, id.ToString(), storeName));
    }
}