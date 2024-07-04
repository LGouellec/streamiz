using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class CacheKeyValueStoreTests
    {
        private StreamConfig config;
        private CachingKeyValueStore cache;
        private ProcessorContext context;
        private TaskId id;
        private TopicPartition partition;
        private ProcessorStateManager stateManager;
        private Mock<AbstractTask> task;
        private InMemoryKeyValueStore inMemoryKeyValue;
        private StreamMetricsRegistry metricsRegistry;
        private string threadId = StreamMetricsRegistry.UNKNOWN_THREAD;
        
        #region Tools
        private Bytes ToKey(string key)
        {
            var serdes = new StringSerDes();
            return Bytes.Wrap(serdes.Serialize(key, SerializationContext.Empty));
        }

        private string FromKey(Bytes bytes)
        {
            var serdes = new StringSerDes();
            return serdes.Deserialize(bytes.Get, SerializationContext.Empty);
        }

        private byte[] ToValue(string value)
        {
            var serdes = new StringSerDes();
            return serdes.Serialize(value, SerializationContext.Empty);
        }

        private string FromValue(byte[] bytes)
        {
            var serdes = new StringSerDes();
            return serdes.Deserialize(bytes, SerializationContext.Empty);
        }
        #endregion
        
        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = "unit-test-cachestore-kv";
            config.DefaultStateStoreCacheMaxBytes = 1000;

            threadId = Thread.CurrentThread.Name ?? StreamMetricsRegistry.UNKNOWN_THREAD;
            id = new TaskId { Id = 0, Partition = 0 };
            partition = new TopicPartition("source", 0);
            stateManager = new ProcessorStateManager(
                id,
                new List<TopicPartition> { partition },
                null,
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);

            metricsRegistry = new StreamMetricsRegistry("test", MetricsRecordingLevel.DEBUG);
            context = new ProcessorContext(task.Object, config, stateManager, metricsRegistry);

            inMemoryKeyValue = new InMemoryKeyValueStore("store");
            cache = new CachingKeyValueStore(inMemoryKeyValue, null);
            cache.Init(context, cache);
        }

        [TearDown]
        public void End()
        {
            if (cache != null)
            {
                cache.Flush();
                stateManager.Close();
            }
        }
        
        [Test]
        public void ExpiryCapacityTest()
        {
            config.DefaultStateStoreCacheMaxBytes = 30;
            cache.CreateCache(context);
            bool checkListener = true;
            cache.SetFlushListener(record => {
                if (checkListener)
                {
                    Assert.AreEqual(ToKey("test").Get, record.Key);
                    Assert.AreEqual(ToValue("value1"), record.Value.NewValue);
                    Assert.IsNull(record.Value.OldValue);
                    checkListener = false;
                }
            }, true);

            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.Put(ToKey("test"), ToValue("value1"));
            cache.Put(ToKey("test2"), ToValue("value2"));
            Assert.AreEqual(ToValue("value1"), inMemoryKeyValue.Get(ToKey("test")));
        }
        
        [Test]
        public void DuplicateValueSameKeyTest()
        {
            cache.SetFlushListener(record => {
                Assert.AreEqual(ToKey("test").Get, record.Key);
                Assert.AreEqual(ToValue("value2"), record.Value.NewValue);
                Assert.IsNull(record.Value.OldValue);
            }, true);

            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.Put(ToKey("test"), ToValue("value1"));
            cache.Put(ToKey("test"), ToValue("value2"));
            cache.Flush();
            Assert.AreEqual(ToValue("value2"), inMemoryKeyValue.Get(ToKey("test")));
        }
        
        [Test]
        public void DuplicateValueWithOldValueSameKeyTest()
        {
            bool checkedListener = false;
            cache.SetFlushListener(record => {
                if (checkedListener)
                {
                    Assert.AreEqual(ToKey("test").Get, record.Key);
                    Assert.AreEqual(ToValue("value2"), record.Value.NewValue);
                    Assert.IsNotNull(record.Value.OldValue);
                    Assert.AreEqual(ToValue("value1"), record.Value.OldValue);
                }
            }, true);

            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.Put(ToKey("test"), ToValue("value1"));
            cache.Flush();
            checkedListener = true;
            cache.Put(ToKey("test"), ToValue("value2"));
            cache.Flush();
            Assert.AreEqual(ToValue("value2"), inMemoryKeyValue.Get(ToKey("test")));
        }
        
        [Test]
        public void FlushTest()
        {
            cache.SetFlushListener(record => {
                Assert.AreEqual(ToKey("test").Get, record.Key);
                Assert.AreEqual(ToValue("value1"), record.Value.NewValue);
                Assert.IsNull(record.Value.OldValue);
            }, true);

            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.Put(ToKey("test"), ToValue("value1"));
            cache.Flush();
            Assert.AreEqual(ToValue("value1"), inMemoryKeyValue.Get(ToKey("test")));
        }
        
        [Test]
        public void DeleteTest()
        {
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.Put(ToKey("test"), ToValue("value1"));
            cache.Flush();
            Assert.AreEqual(ToValue("value1"), cache.Get(ToKey("test")));
            Assert.AreEqual(ToValue("value1"), inMemoryKeyValue.Get(ToKey("test")));
            cache.Delete(ToKey("test"));
            Assert.IsNull(cache.Get(ToKey("test")));
            cache.Flush();
            Assert.IsNull(inMemoryKeyValue.Get(ToKey("test")));
        }
        
        [Test]
        public void PutAllTest()
        {
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            var input = new List<KeyValuePair<Bytes, byte[]>>
            {
                new(ToKey("test1"), ToValue("value1")),
                new(ToKey("test2"), ToValue("value2")),
                new(ToKey("test2"), ToValue("value2bis")),
                new(ToKey("test3"), ToValue("value3")),
            };
            cache.PutAll(input);
            Assert.AreEqual(3, cache.ApproximateNumEntries());
            cache.Flush();
            Assert.AreEqual(0, cache.ApproximateNumEntries());
            Assert.AreEqual(3, inMemoryKeyValue.ApproximateNumEntries());
            Assert.AreEqual(ToValue("value1"), inMemoryKeyValue.Get(ToKey("test1")));
            Assert.AreEqual(ToValue("value2bis"), inMemoryKeyValue.Get(ToKey("test2")));
            Assert.AreEqual(ToValue("value3"), inMemoryKeyValue.Get(ToKey("test3")));
        }
        
        [Test]
        public void PutIfAbsentTest()
        {
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.PutIfAbsent(ToKey("test"), ToValue("value1"));
            cache.PutIfAbsent(ToKey("test"), ToValue("value2"));
            cache.Flush();
            Assert.AreEqual(ToValue("value1"), inMemoryKeyValue.Get(ToKey("test")));
        }

        [Test]
        public void RangeTest()
        {
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.PutAll(new List<KeyValuePair<Bytes, byte[]>>
            {
                new(ToKey("test1"), ToValue("value1")),
                new(ToKey("test2"), ToValue("value2")),
                new(ToKey("test3"), ToValue("value3")),
                new(ToKey("test4"), ToValue("value4"))
            });
            
            Assert.AreEqual(4, cache.ApproximateNumEntries());

            Assert.AreEqual(4, cache.All().ToList().Count);
            Assert.AreEqual(4, cache.ReverseAll().ToList().Count);
            Assert.AreEqual(2, cache.Range(ToKey("test"), ToKey("test2")).ToList().Count);
            Assert.AreEqual(2, cache.ReverseRange(ToKey("test"), ToKey("test2")).ToList().Count);
        }
        
        [Test]
        public void DisabledCachingTest()
        {
            config.DefaultStateStoreCacheMaxBytes = 0;
            cache.CreateCache(context);
            
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.Put(ToKey("test"), ToValue("value1"));
            cache.Put(ToKey("test2"), ToValue("value2"));
            
            Assert.AreEqual(ToValue("value1"), cache.Get(ToKey("test")));
            Assert.AreEqual(ToValue("value2"), cache.Get(ToKey("test2")));
            Assert.AreEqual(2, cache.ApproximateNumEntries());

            cache.Delete(ToKey("test"));
            Assert.AreEqual(1, cache.ApproximateNumEntries());

            Assert.Null(cache.Get(ToKey("test")));
            Assert.Null(cache.PutIfAbsent(ToKey("test"), ToValue("value1")));
            
            cache.PutAll(new List<KeyValuePair<Bytes, byte[]>>
            {
                new(ToKey("test3"), ToValue("value3")),
                new(ToKey("test4"), ToValue("value4"))
            });
            
            Assert.AreEqual(4, cache.ApproximateNumEntries());

            Assert.AreEqual(4, cache.All().ToList().Count);
            Assert.AreEqual(4, cache.ReverseAll().ToList().Count);
            Assert.AreEqual(4, cache.Range(ToKey("test"), ToKey("test4")).ToList().Count);
            Assert.AreEqual(4, cache.ReverseRange(ToKey("test"), ToKey("test4")).ToList().Count);
        }


        [Test]
        public void TestMetrics()
        {
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.PutAll(new List<KeyValuePair<Bytes, byte[]>>
            {
                new(ToKey("test1"), ToValue("value1")),
                new(ToKey("test2"), ToValue("value2")),
                new(ToKey("test3"), ToValue("value3")),
                new(ToKey("test4"), ToValue("value4"))
            });

            cache.Get(ToKey("test1"));
            cache.Get(ToKey("test1"));
            cache.Get(ToKey("test50")); // not found
            cache.Get(ToKey("test100")); // not found

            var totalCacheSize = GetSensorMetric(
                CachingMetrics.CACHE_SIZE_BYTES_TOTAL,
                string.Empty,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP);
            
            var hitRatioAvg = GetSensorMetric(
                CachingMetrics.HIT_RATIO,
                "-avg",
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP);
            
            Assert.IsTrue((double)totalCacheSize.Value > 0);
            Assert.IsTrue((double)hitRatioAvg.Value > 0.5d);
        }
        
        private StreamMetric GetSensorMetric(string sensorName, string metricSuffix, string group)
        {
            long now = DateTime.Now.GetMilliseconds();
            var sensor = metricsRegistry.GetSensors().FirstOrDefault(s => s.Name.Equals(GetSensorName(sensorName)));
            if (sensor == null)
                throw new NullReferenceException($"sensor {sensorName} not found");

            MetricName keyMetric = MetricName.NameAndGroup(
                sensorName + metricSuffix,
                group);
            
            if (!sensor.Metrics.ContainsKey(keyMetric))
                throw new NullReferenceException($"metric {sensorName + metricSuffix}|{group} not found inside {sensorName}");
            
            return sensor.Metrics[keyMetric];
        }
        
        [Test]
        public void RehydrateCachingBasedOnWrappedStoreTest()
        {
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            inMemoryKeyValue.Put(ToKey("test"), ToValue("value1"));
            inMemoryKeyValue.Put(ToKey("test2"), ToValue("value2"));
            Assert.AreEqual(0, cache.ApproximateNumEntries());
            
            Assert.AreEqual(ToValue("value1"), cache.Get(ToKey("test")));
            Assert.AreEqual(ToValue("value2"), cache.Get(ToKey("test2")));
            Assert.AreEqual(2, cache.ApproximateNumEntries());
            cache.Flush(); 
            
            cache.Delete(ToKey("test"));
            Assert.AreEqual(1, cache.ApproximateNumEntries());
            Assert.AreEqual(2, inMemoryKeyValue.ApproximateNumEntries());

            cache.Flush();
            Assert.AreEqual(1, inMemoryKeyValue.ApproximateNumEntries());
        }

        
        private string GetSensorName(string sensorName)
            => metricsRegistry.FullSensorName(
                sensorName,
                metricsRegistry.StoreSensorPrefix(threadId, id.ToString(), "store"));
    }
}