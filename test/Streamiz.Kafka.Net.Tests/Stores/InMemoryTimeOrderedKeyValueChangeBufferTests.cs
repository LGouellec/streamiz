using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Suppress.Internal;
using Moq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class InMemoryTimeOrderedKeyValueChangeBufferTests
{
    private InMemoryTimeOrderedKeyValueChangeBuffer<string, string> buffer;
    private ProcessorContext context;
    private StreamsProducer producer;
    private Action<ConsumeResult<byte[],byte[]>> restoreCallback;

    private void PutRecord(long streamTime,
        long recordTimestamp,
        String key,
        String value)
    {
        RecordContext recordContext = new RecordContext(new Headers(), 0, recordTimestamp, 0, "topic");
        context.SetRecordMetaData(recordContext);
        buffer.Put(streamTime, key, new Change<string>(null, value), recordContext);
    }

    [SetUp]
    public void Init()
    {
        buffer = new InMemoryTimeOrderedKeyValueChangeBuffer<string, string>(
            "suppress-store",
            true,
            null,
            null);

        var config = new StreamConfig();
        config.ApplicationId = "suppress-in-memory-store";

        var metricsRegistry = new StreamMetricsRegistry();
        
        var stateManager = new Mock<IStateManager>();
        stateManager.Setup(s => 
                s.Register(It.IsAny<IStateStore>(), It.IsAny<Action<ConsumeResult<byte[], byte[]>>>()))
            .Callback((IStateStore store, Action<ConsumeResult<byte[], byte[]>> callback) =>
            {
                restoreCallback = callback;
            });
        
        stateManager.Setup(s => s.GetRegisteredChangelogPartitionFor(It.IsAny<string>()))
            .Returns((string stateStore) => new TopicPartition(stateStore + "-changelog", 0));

        var supplier = new SyncKafkaSupplier();
        producer = new StreamsProducer(
            config,
            "thread",
            Guid.NewGuid(),
            supplier,
            "");
            
        var collector = new RecordCollector(
            "test-collector",
            config,
            new TaskId {Id = 0, Partition = 0},
            producer,
            NoRunnableSensor.Empty);
        
        var context = new Mock<ProcessorContext>();
        context.Setup(c => c.Id)
            .Returns(new TaskId { Id = 0, Partition = 0 });
        context.Setup(c => c.Configuration)
            .Returns(config);
        context.Setup(c => c.Metrics)
            .Returns(() => metricsRegistry);
        context.Setup(c => c.States)
            .Returns(() => stateManager.Object);
        context.Setup(c => c.RecordCollector)
            .Returns(() => collector);

        this.context = context.Object;
        buffer.Init(this.context, buffer);
    }

    [TearDown]
    public void Dispose()
    {
        buffer.Close();
    }

    private static BufferValue GetBufferValue(String value, long timestamp) {
        return new BufferValue(
            null,
            null,
            Encoding.UTF8.GetBytes(value),
            new RecordContext(new Headers(), 0, timestamp, 0, "topic")
        );
    }
    
    [Test]
    public void AcceptDataTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        PutRecord(0L, 0L, "key1", "value1");
        Assert.AreEqual(1, buffer.NumRecords);
    }

    [Test]
    public void RejectNullValuesTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        RecordContext recordContext = new RecordContext(new Headers(), 0, 0L, 0, "topic");
        Assert.Throws<ArgumentNullException>(() => buffer.Put(0L, "key", null, recordContext));
    }

    [Test]
    public void RejectNullContextTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        Assert.Throws<ArgumentNullException>(() => buffer.Put(0L, "key", new Change<string>(null, "value"), null));
    }

    [Test]
    public void RemoveDataTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        PutRecord(0L, 0L, "key1", "value1");
        Assert.AreEqual(1, buffer.NumRecords);
        buffer.EvictWhile(() => true, (_, _, _) => { });
        Assert.AreEqual(0, buffer.NumRecords);
    }
    
    [Test]
    public void RespectEvictionPredicateTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        PutRecord(0L, 0L, "key1", "value1");
        PutRecord(1L, 0L, "key2", "value2");
        Assert.AreEqual(2, buffer.NumRecords);
        List<(string, string)> evicted = new();
        List<(string, string)> expected = new()
        {
            ("key1", "value1")
        };
        
        buffer.EvictWhile(() => buffer.NumRecords > 1, (k, v, _) => { evicted.Add((k, v.NewValue));});
        
        Assert.AreEqual(1, buffer.NumRecords);
        Assert.AreEqual(expected, evicted);
    }

    [Test]
    public void TrackCountTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        
        PutRecord(0L, 0L, "key1", "value1");
        Assert.AreEqual(1, buffer.NumRecords);
        PutRecord(1L, 0L, "key2", "value2");
        Assert.AreEqual(2, buffer.NumRecords);
        PutRecord(2L, 0L, "key3", "value3");
        Assert.AreEqual(3, buffer.NumRecords);
    }
    
    [Test]
    public void TrackSizeTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        
        // 8 + 10 (key/value) + 25 (timestamp + tpo + headers) 
        PutRecord(0L, 0L, "key1", "value1");
        Assert.AreEqual(43, buffer.BufferSize);
        // 8 + 6 (key/value) + 25 (timestamp + tpo + headers) 
        PutRecord(1L, 0L, "key1", "v2");
        Assert.AreEqual(39, buffer.BufferSize);
        // 8 + 10 (key/value) + 25 (timestamp + tpo + headers) 
        PutRecord(2L, 0L, "key3", "value3");
        Assert.AreEqual(82, buffer.BufferSize);
    }

    [Test]
    public void TrackMinTimestampTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        
        PutRecord(1L, 0L, "key1", "v1");
        Assert.AreEqual(1, buffer.MinTimestamp);
        PutRecord(0L, 0L, "key2", "v2");
        Assert.AreEqual(0, buffer.MinTimestamp);
    }

    [Test]
    public void EvictOldestAndUpdateSizeAndCountAndMinTimestampTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        
        PutRecord(1L, 0L, "key1", "12345");
        Assert.AreEqual(42, buffer.BufferSize);
        Assert.AreEqual(1, buffer.NumRecords);
        Assert.AreEqual(1, buffer.MinTimestamp);
        
        PutRecord(0L, 0L, "key2", "123");
        Assert.AreEqual(82, buffer.BufferSize);
        Assert.AreEqual(2, buffer.NumRecords);
        Assert.AreEqual(0, buffer.MinTimestamp);
        
        int @case = 0;
        buffer.EvictWhile(() => true, (k, v, r) =>
        {
            ++@case;
            if (@case == 1)
            {
                Assert.AreEqual("key2", k);
                Assert.AreEqual(2, buffer.NumRecords);
                Assert.AreEqual(82, buffer.BufferSize);
                Assert.AreEqual(0, buffer.MinTimestamp);
            }
            else if (@case == 2)
            {
                Assert.AreEqual("key1", k);
                Assert.AreEqual(1, buffer.NumRecords);
                Assert.AreEqual(42, buffer.BufferSize);
                Assert.AreEqual(1, buffer.MinTimestamp);
            }
        });
        
        Assert.AreEqual(2, @case);
        Assert.AreEqual(0, buffer.NumRecords);
        Assert.AreEqual(0, buffer.BufferSize);
        Assert.AreEqual(long.MaxValue, buffer.MinTimestamp);
    }

    [Test]
    public void ReturnUndefinedOnPriorValueForNotBufferedKeyTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        
        Assert.IsFalse(buffer.PriorValueForBuffered("ASDF").IsDefined);
    }

    [Test]
    public void ReturnPriorValueForBufferedKeyTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        
        RecordContext recordContext = new RecordContext(new Headers(), 0, 0L, 0, "topic");
        context.SetRecordMetaData(recordContext);
        
        buffer.Put(1L, "A", new Change<string>("old-value", "new-value"), recordContext);
        buffer.Put(1L, "B", new Change<string>(null, "new-value"), recordContext);

        Assert.AreEqual("old-value", buffer.PriorValueForBuffered("A").Value.Value);
        Assert.AreEqual(-1, buffer.PriorValueForBuffered("A").Value.Timestamp);
    }

    [Test]
    public void FlushTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        
        PutRecord(2L, 0L, "key1", "value1");
        PutRecord(1L, 1L, "key2", "value2");
        PutRecord(0L, 2L, "key3", "value3");
        
        buffer.EvictWhile(() => buffer.MinTimestamp < 1, (k,v,c) => { });
        buffer.Flush();

        var messages = ((SyncProducer)producer.Producer)
            .GetHistory("suppress-store-changelog")
            .Select(m =>
            {
                if (m.Value == null)
                    return (-1, null);
                
                var byteBuffer = ByteBuffer.Build(m.Value, true);
                var bufferValue = BufferValue.Deserialize(byteBuffer);
                long timestamp = byteBuffer.GetLong();
                return (timestamp, bufferValue);
            })
            .ToList();

        List<(long, BufferValue)> expectedMsg = new()
        {
            (2, GetBufferValue("value1", 0L)),
            (1, GetBufferValue("value2", 1L)),
            (-1, null),
        };
        
        Assert.AreEqual(expectedMsg, messages);
    }

    [Test]
    public void RestoreTest()
    {
        buffer.SetSerdesIfNull(new StringSerDes(), new StringSerDes());
        
        BufferValue GetBufferValueWithHeaders(string v, long timestamp)
        {
            var bufferValue = GetBufferValue(v, timestamp);
            bufferValue.RecordContext.Headers.Add("header", Encoding.UTF8.GetBytes("header-value"));
            return bufferValue;
        }
        
        var bufferValue1 = GetBufferValueWithHeaders("value1", 0L).Serialize(sizeof(long));
        bufferValue1.PutLong(0L);
        var bufferValue2 = GetBufferValueWithHeaders("value2", 1L).Serialize(sizeof(long));
        bufferValue2.PutLong(1L);
        var bufferValue3 = GetBufferValueWithHeaders("value3", 2L).Serialize(sizeof(long));
        bufferValue3.PutLong(2L);

        BufferValue fb2bis = new BufferValue(
            Encoding.UTF8.GetBytes("value2"),
            null,
            Encoding.UTF8.GetBytes("value2bis"),
            new RecordContext(new Headers(), 0, 3L, 0, "topic"));
        var bufferValue2Bis = fb2bis.Serialize(sizeof(long));
        bufferValue2Bis.PutLong(3L);

        var headerSizeBytes = 18L;
        
        List<ConsumeResult<byte[], byte[]>> recordsToRestore = new()
        {
            new ConsumeResult<byte[], byte[]>
            {
                // 4 + 8 + 8 + 5
                TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 0),
                Message = new Message<byte[], byte[]>()
                {
                    Key = Encoding.UTF8.GetBytes("key1"), // 4
                    Value = bufferValue1.ToArray(), // 6 + 8
                    Timestamp = new Timestamp(0L, TimestampType.CreateTime),
                    Headers = new()
                },
                IsPartitionEOF = false
            },
            new ConsumeResult<byte[], byte[]>
            {
                // 4 + 8 + 8 + 5
                TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 1),
                Message = new Message<byte[], byte[]>()
                {
                    Key = Encoding.UTF8.GetBytes("key2"), // 4
                    Value = bufferValue2.ToArray(), // 6 +8
                    Timestamp = new Timestamp(1L, TimestampType.CreateTime),
                    Headers = new()
                },
                IsPartitionEOF = false
            },
            new ConsumeResult<byte[], byte[]>
            {
                // 4 + 8 + 8 + 5
                TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 2),
                Message = new Message<byte[], byte[]>()
                {
                    Key = Encoding.UTF8.GetBytes("key3"), // 4
                    Value = bufferValue3.ToArray(), // 6 + 8
                    Timestamp = new Timestamp(2L, TimestampType.CreateTime),
                    Headers = new()
                },
                IsPartitionEOF = false
            }
        };

        foreach (var r in recordsToRestore)
            restoreCallback(r);
        
        Assert.AreEqual(3, buffer.NumRecords);
        Assert.AreEqual(0, buffer.MinTimestamp);
        Assert.AreEqual(129 + 3*headerSizeBytes, buffer.BufferSize);

        restoreCallback(new ConsumeResult<byte[], byte[]>
        {
            // 4 + 8 + 8 + 5
            TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 2),
            Message = new Message<byte[], byte[]>()
            {
                Key = Encoding.UTF8.GetBytes("key3"), // 4
                Value = null,
                Timestamp = new Timestamp(3L, TimestampType.CreateTime),
                Headers = new()
            },
            IsPartitionEOF = false
        });
        
        Assert.AreEqual(2, buffer.NumRecords);
        Assert.AreEqual(0, buffer.MinTimestamp);
        Assert.AreEqual(86 + 2*headerSizeBytes, buffer.BufferSize);
        
        restoreCallback(new ConsumeResult<byte[], byte[]>
        {
            // 4 + 8 + 8 + 5
            TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 4),
            Message = new Message<byte[], byte[]>()
            {
                Key = Encoding.UTF8.GetBytes("key2"), // 4
                Value = bufferValue2Bis.ToArray(),
                Timestamp = new Timestamp(3L, TimestampType.CreateTime),
                Headers = new()
            },
            IsPartitionEOF = false
        });

        Assert.IsFalse(buffer.PriorValueForBuffered("key3").IsDefined);
        Assert.Throws<IllegalStateException>(() =>
        {
            var value = buffer.PriorValueForBuffered("key3").Value;
        });
        Assert.IsTrue(buffer.PriorValueForBuffered("key1").IsDefined);
        Assert.IsNull(buffer.PriorValueForBuffered("key1").Value);
        Assert.IsTrue(buffer.PriorValueForBuffered("key2").IsDefined);
        Assert.AreEqual("value2", buffer.PriorValueForBuffered("key2").Value.Value);
    }

    [Test]
    public void UseDefaultSerdesTest()
    {
        context.Configuration.DefaultKeySerDes = new StringSerDes();
        context.Configuration.DefaultValueSerDes = new StringSerDes();
        buffer.SetSerdesIfNull(null, null);
    }
    
    [Test]
    public void KeySerdesStillNullTest()
    {
        context.Configuration.DefaultValueSerDes = new StringSerDes();
        Assert.Throws<StreamsException>(() => buffer.SetSerdesIfNull(null, null));
    }
    
    [Test]
    public void ValueSerdesStillNullTest()
    {
        context.Configuration.DefaultKeySerDes = new StringSerDes();
        Assert.Throws<StreamsException>(() => buffer.SetSerdesIfNull(null, null));
    }
}