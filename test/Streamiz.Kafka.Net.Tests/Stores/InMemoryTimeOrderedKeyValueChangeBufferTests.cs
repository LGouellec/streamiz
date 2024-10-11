using System;
using System.Collections.Generic;
using Castle.Core.Logging;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Suppress.Internal;
using Moq;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class InMemoryTimeOrderedKeyValueChangeBufferTests
{
    private InMemoryTimeOrderedKeyValueChangeBuffer<string, string> buffer;
    private ProcessorContext context;

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
            new StringSerDes(),
            new StringSerDes());

        var config = new StreamConfig();
        config.ApplicationId = "suppress-in-memory-store";

        var metricsRegistry = new StreamMetricsRegistry();

        var stateManager = new Mock<IStateManager>();
        stateManager.Setup(s =>
            s.Register(It.IsAny<IStateStore>(), It.IsAny<Action<ConsumeResult<byte[], byte[]>>>()));

        var context = new Mock<ProcessorContext>();
        context.Setup(c => c.Id)
            .Returns(new TaskId { Id = 0, Partition = 0 });
        context.Setup(c => c.Configuration)
            .Returns(config);
        context.Setup(c => c.Metrics)
            .Returns(() => metricsRegistry);
        context.Setup(c => c.States)
            .Returns(() => stateManager.Object);

        this.context = context.Object;
        buffer.Init(this.context, buffer);
    }

    [TearDown]
    public void Dispose()
    {
        buffer.Close();
    }

    [Test]
    public void AcceptDataTest()
    {
        PutRecord(0L, 0L, "key1", "value1");
        Assert.AreEqual(1, buffer.NumRecords);
    }

    [Test]
    public void RejectNullValuesTest()
    {
        RecordContext recordContext = new RecordContext(new Headers(), 0, 0L, 0, "topic");
        Assert.Throws<ArgumentNullException>(() => buffer.Put(0L, "key", null, recordContext));
    }

    [Test]
    public void RejectNullContextTest()
    {
        Assert.Throws<ArgumentNullException>(() => buffer.Put(0L, "key", new Change<string>(null, "value"), null));
    }

    [Test]
    public void RemoveDataTest()
    {
        PutRecord(0L, 0L, "key1", "value1");
        Assert.AreEqual(1, buffer.NumRecords);
        buffer.EvictWhile(() => true, (_, _, _) => { });
        Assert.AreEqual(0, buffer.NumRecords);
    }
    
    [Test]
    public void RespectEvictionPredicateTest()
    {
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
        
    }
}