using System.Collections.Generic;
using Confluent.Kafka;
using NUnit.Framework;
using Moq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class CacheKeyValueStoreTests
    {
        private StreamConfig config = null;
        private CachingKeyValueStore cache = null;
        private ProcessorContext context = null;
        private TaskId id = null;
        private TopicPartition partition = null;
        private ProcessorStateManager stateManager = null;
        private Mock<AbstractTask> task = null;
        private InMemoryKeyValueStore inMemoryKeyValue = null;
        
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
            config.ApplicationId = $"unit-test-cachestore-kv";
            config.StateStoreCacheMaxBytes = 1000;

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

            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());

            inMemoryKeyValue = new InMemoryKeyValueStore("store");
            cache = new CachingKeyValueStore(inMemoryKeyValue);
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
            config.StateStoreCacheMaxBytes = 10;
            cache.CreateCache(context);
            
            cache.SetFlushListener((record) => {
                Assert.AreEqual(ToKey("test").Get, record.Key);
                Assert.AreEqual(ToValue("value1"), record.Value.NewValue);
                Assert.IsNull(record.Value.OldValue);
            }, true);

            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "topic"));
            cache.Put(ToKey("test"), ToValue("value1"));
            Assert.AreEqual(ToValue("value1"), inMemoryKeyValue.Get(ToKey("test")));
        }
        
        [Test]
        public void DuplicateValueSameKeyTest()
        {
            cache.SetFlushListener((record) => {
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
            cache.SetFlushListener((record) => {
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
            cache.SetFlushListener((record) => {
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
        // implement and test range methods        
    }
}