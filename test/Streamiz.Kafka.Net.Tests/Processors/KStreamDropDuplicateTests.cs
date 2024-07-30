using System;
using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors;

public class KStreamDropDuplicateTests
{
    [Test]
    public void NoDuplicate()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.ApplicationId = "test-drop-duplicate";

        var builder = new StreamBuilder();
        Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m = null;

        builder
            .Stream<string, string>("input")
            .DropDuplicate((k,v1,v2) => v1.Equals(v2),
                TimeSpan.FromMinutes(1),
                InMemoryWindows.As<string, string>("duplicate-store")
                    .WithCachingDisabled())
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), config);
        var input = driver.CreateInputTopic<string, string>("input");
        var output = driver.CreateOuputTopic<string, string>("output");
        input.PipeInput("key1", "test1");
        input.PipeInput("key2", "test2");
        var records = output.ReadKeyValueList().ToList();
        Assert.AreEqual(2, records.Count);
        
        Assert.AreEqual("key1", records[0].Message.Key);
        Assert.AreEqual("test1", records[0].Message.Value);
        Assert.AreEqual(0, records[0].Offset.Value);
        
        Assert.AreEqual("key2", records[1].Message.Key);
        Assert.AreEqual("test2", records[1].Message.Value);
        Assert.AreEqual(1, records[1].Offset.Value);
    }
    
    [Test]
    public void DuplicateSameKeyValue()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.ApplicationId = "test-drop-duplicate";

        var builder = new StreamBuilder();
        Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m = null;

        builder
            .Stream<string, string>("input")
            .DropDuplicate((k,v1,v2) => v1.Equals(v2),
                TimeSpan.FromMinutes(1),
                InMemoryWindows.As<string, string>("duplicate-store")
                    .WithCachingDisabled())
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), config);
        var input = driver.CreateInputTopic<string, string>("input");
        var output = driver.CreateOuputTopic<string, string>("output");
        input.PipeInput("key1", "test1");
        input.PipeInput("key1", "test1");
        var records = output.ReadKeyValueList().ToList();
        Assert.AreEqual(1, records.Count);
        
        Assert.AreEqual("key1", records[0].Message.Key);
        Assert.AreEqual("test1", records[0].Message.Value);
        Assert.AreEqual(0, records[0].Offset.Value);
    }
    
    [Test]
    public void NoDuplicateSameKeyButDifferentValue()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.ApplicationId = "test-drop-duplicate";

        var builder = new StreamBuilder();
        Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m = null;

        builder
            .Stream<string, string>("input")
            .DropDuplicate((k,v1,v2) => v1.Equals(v2),
                TimeSpan.FromMinutes(1),
                InMemoryWindows.As<string, string>("duplicate-store")
                    .WithCachingDisabled())
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), config);
        var input = driver.CreateInputTopic<string, string>("input");
        var output = driver.CreateOuputTopic<string, string>("output");
        input.PipeInput("key1", "test1");
        input.PipeInput("key1", "test2");
        var records = output.ReadKeyValueList().ToList();
        Assert.AreEqual(2, records.Count);
        
        Assert.AreEqual("key1", records[0].Message.Key);
        Assert.AreEqual("test1", records[0].Message.Value);
        Assert.AreEqual(0, records[0].Offset.Value);
        
        Assert.AreEqual("key1", records[1].Message.Key);
        Assert.AreEqual("test2", records[1].Message.Value);
        Assert.AreEqual(1, records[1].Offset.Value);
    }
}