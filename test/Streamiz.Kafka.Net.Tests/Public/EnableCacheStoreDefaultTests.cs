using System;
using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Public;

public class EnableCacheStoreDefaultTests
{
    [Test]
    public void EnableCacheStoreByDefaultTest()
    {
        var streamConfig = new StreamConfig<StringSerDes, StringSerDes>();
        streamConfig.ApplicationId = "test-cache-store-default";
        streamConfig.EnableCacheStoreByDefault(CacheSize.OfKb(10));
        
        var builder = new StreamBuilder();

        builder
            .Stream<string, string>("topic")
            .GroupByKey()
            .Count(InMemory.As<string, long>("count-store").WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .To("output-count", new StringSerDes(), new Int64SerDes());

        var topology = builder.Build();
        using var driver = new TopologyTestDriver(topology, streamConfig);
        var input = driver.CreateInputTopic<string, string>("topic");
        var output = driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("output-count");
        input.PipeInput("test", "1");
        input.PipeInput("test", "2");
        input.PipeInput("test", "3");
        var records = output.ReadKeyValueList().ToList();
        Assert.AreEqual(0, records.Count);
        driver.Commit();
        var records2 = output.ReadKeyValueList().ToList();
        Assert.AreEqual(1, records2.Count);
        Assert.AreEqual("test", records2[0].Message.Key);
        Assert.AreEqual(3L, records2[0].Message.Value);
    }
    
    [Test]
    public void ExplicitallyDisableCacheEvenIfCacheStoreByDefaultIsEnabledTest()
    {
        var streamConfig = new StreamConfig<StringSerDes, StringSerDes>();
        streamConfig.ApplicationId = "test-cache-store-default";
        streamConfig.EnableCacheStoreByDefault(CacheSize.OfKb(10));
        
        var builder = new StreamBuilder();

        builder
            .Stream<string, string>("topic")
            .GroupByKey()
            .Count(
                InMemory
                    .As<string, long>("count-store")
                    .WithValueSerdes(new Int64SerDes())
                    .WithCachingDisabled())
            .ToStream()
            .To("output-count", new StringSerDes(), new Int64SerDes());

        var topology = builder.Build();
        using var driver = new TopologyTestDriver(topology, streamConfig);
        var input = driver.CreateInputTopic<string, string>("topic");
        var output = driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("output-count");
        input.PipeInput("test", "1");
        input.PipeInput("test", "2");
        input.PipeInput("test", "3");
        var records = output.ReadKeyValueList().ToList();
        Assert.AreEqual(3, records.Count);
        Assert.AreEqual("test", records[0].Message.Key);
        Assert.AreEqual(1L, records[0].Message.Value);
        Assert.AreEqual("test", records[1].Message.Key);
        Assert.AreEqual(2L, records[1].Message.Value);
        Assert.AreEqual("test", records[2].Message.Key);
        Assert.AreEqual(3L, records[2].Message.Value);
    }
    
    
    [Test]
    public void EnableCacheStoreByDefaultWithWindowStoreTest()
    {
        var streamConfig = new StreamConfig<StringSerDes, StringSerDes>();
        streamConfig.ApplicationId = "test-cache-store-default";
        streamConfig.EnableCacheStoreByDefault(CacheSize.OfKb(10));
        
        var builder = new StreamBuilder();

        builder
            .Stream<string, string>("topic")
            .GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(5)))
            .Count(InMemoryWindows.As<string, long>("count-store").WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .SelectKey((k,v,r) => k.Key)
            .To("output-count", new StringSerDes(), new Int64SerDes());

        var topology = builder.Build();
        var dt = DateTime.Now;
        using var driver = new TopologyTestDriver(topology, streamConfig);
        var input = driver.CreateInputTopic<string, string>("topic");
        var output = driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("output-count");
        input.PipeInput("test", "1", dt);
        input.PipeInput("test", "2", dt.AddSeconds(1));
        input.PipeInput("test", "3", dt.AddSeconds(2));
        input.PipeInput("test", "4", dt.AddSeconds(7));
        var records = output.ReadKeyValueList().ToList();
        Assert.AreEqual(0, records.Count);
        driver.Commit();
        var records2 = output.ReadKeyValueList().ToList();
        Assert.AreEqual(2, records2.Count);
        Assert.AreEqual("test", records2[0].Message.Key);
        Assert.AreEqual(3L, records2[0].Message.Value);
        Assert.AreEqual("test", records2[1].Message.Key);
        Assert.AreEqual(1L, records2[1].Message.Value);
    }
}