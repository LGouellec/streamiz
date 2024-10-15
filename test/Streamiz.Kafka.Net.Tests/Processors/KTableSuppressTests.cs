using System;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors;

public class KTableSuppressTests
{
    private StreamConfig streamConfig;
    private DateTime dt;

    [SetUp]
    public void Init()
    {
        streamConfig = new StreamConfig();
        streamConfig.ApplicationId = "test-ktable-suppress-test";
        
        var c = CultureInfo.CreateSpecificCulture("fr-FR");
        dt = DateTime.Parse("15/10/2024 08:00:00", c);
    }
    
    private StreamBuilder BuildTopology(Suppressed<Windowed<string>, long> suppressed)
    {
        var builder = new StreamBuilder();
        
        builder.Stream("input", new StringSerDes(), new StringSerDes())
            .GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
            .Count(
                InMemoryWindows
                    .As<string, long>("count-store")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes()))
            .Suppress(suppressed)
            .ToStream()
            .To("output", 
                new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds), 
                new Int64SerDes());

        return builder;
    }
    
    [Test]
    public void WindowedZeroTimeLimitShouldImmediatelyEmit()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilTimeLimit<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", DateTime.Now);
        inputTopic.PipeInput("key1", "value2", DateTime.Now);

        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(2, records.Count());
    }

    [Test]
    public void IntermediateSuppressionShouldBufferAndEmitLater()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilTimeLimit<Windowed<string>, long>(TimeSpan.FromSeconds(1),
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        inputTopic.PipeInput("key1", "value2", dt.AddSeconds(30)); // should process and emit

        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual(2, records.First().Message.Value);
    }

    [Test]
    public void FinalResultsSuppressionShouldBufferAndEmitAtGraceExpiration()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromMinutes(1),
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        
        // although the stream time is now 15/10/2024 08:01:00, we have to wait 1 minutes after the window *end* before we
        // emit "key1", so we don't emit yet.
        inputTopic.PipeInput("key2", "value1", dt.AddMinutes(1));
        
        // ok, now it's time to emit "key1"
        inputTopic.PipeInput("key3", "value1", dt.AddMinutes(2).AddSeconds(1));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void FinalResultsWithZeroGraceShouldStillBufferUntilTheWindowEnd()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        
        // although the stream time is now 15/10/2024 08:01:00, we have to wait 1 minutes after the window *end* before we
        // emit "key1", so we don't emit yet.
        inputTopic.PipeInput("key2", "value1", dt.AddMinutes(1));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void FinalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        
        // although the stream time is now 15/10/2024 08:01:00, we have to wait 1 minutes after the window *end* before we
        // emit "key1", so we don't emit yet.
        inputTopic.PipeInput("key2", "value1", dt.AddMinutes(1));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void SuppressShouldEmitWhenOverRecordCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    EagerConfig.EmitEarlyWhenFull(1)));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void SuppressShouldEmitWhenOverByteCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    EagerConfig.EmitEarlyWhenFull(CacheSize.OfB(80))));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void SuppressShouldShutDownWhenOverRecordCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    StrictBufferConfig.Bounded(1)));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);

        Assert.Throws<StreamsException>(() => inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2)));
    }

    [Test]
    public void SuppressShouldShutDownWhenOverByteCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    StrictBufferConfig.Bounded(CacheSize.OfB(80))));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOuputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);

        Assert.Throws<StreamsException>(() => inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2)));
    }
}