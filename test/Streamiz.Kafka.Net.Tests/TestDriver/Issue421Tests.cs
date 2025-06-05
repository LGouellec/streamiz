using System;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.TestDriver;

public class Issue421Tests
{
    private StreamBuilder _builder;
    private TopologyTestDriver? _client;
    private TestInputTopic<string, string>? _inputTopic;
    private TestOutputTopic<string, string>? _outputTopic;

    private const string APPLICATION_ID = "global-restore-test";
    private const string STATE_STORE_NAME = "state";
    private const string GLOBAL_STATE_STORE_NAME = "state-global";
    private const string INPUT_TOPIC = "events";
    private static string OUTPUT_TOPIC => $"{APPLICATION_ID}-{STATE_STORE_NAME}-changelog";

    private StreamConfig config => new StreamConfig<StringSerDes, StringSerDes> {
        ApplicationId = APPLICATION_ID,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        AllowAutoCreateTopics = true,
        PollMs = 10,
        Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
    };

    [SetUp]
    public void Init()
    {
        _builder = new();

        _builder.Stream<string, string>(INPUT_TOPIC)
            .Peek((k, v, c) => Console.WriteLine($"Event => Key: {k}, Value: {v}"))
            .GroupByKey()
            .Aggregate(
                () => "",
                (k, v, r) => v,
                InMemory.As<string, string>(STATE_STORE_NAME))
            .ToStream()
            .Peek((k, v, c) => Console.WriteLine($"State => Key: {k}, Value: {v}"));
        
        _builder.GlobalTable(OUTPUT_TOPIC, InMemory.As<string, string>(GLOBAL_STATE_STORE_NAME));
    }

    [TearDown]
    public void Dispose()
    {
        _client?.Dispose();
    }

    [Theory]
    [NonParallelizable]
    [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, STATE_STORE_NAME)]
    [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, GLOBAL_STATE_STORE_NAME)]
    [TestCase(TopologyTestDriver.Mode.SYNC_TASK, STATE_STORE_NAME)]
    [TestCase(TopologyTestDriver.Mode.SYNC_TASK, GLOBAL_STATE_STORE_NAME)]
    public void TestGlobalStore(TopologyTestDriver.Mode mode, string assertStoreName)
    {
        _client = new TopologyTestDriver(_builder.Build(), config, mode);
        _inputTopic = _client.CreateInputTopic<string, string>(INPUT_TOPIC);
        _outputTopic = _client.CreateOuputTopic<string, string>(OUTPUT_TOPIC);

        ProduceMessage(INPUT_TOPIC, "key1", "value1");
        ProduceMessage(INPUT_TOPIC, "key2", "value2");
        ProduceMessage(INPUT_TOPIC, "key3", "value3");

        // Waiting the stream thread + global thread processed the messages
        Thread.Sleep((int)config.PollMs * 5);
        
        var store = _client.GetKeyValueStore<string, string>(assertStoreName);
        var items = store.All();

        Assert.AreEqual(3, items.Count());
        Assert.IsTrue(items.Any(item => item.Key == "key1" && item.Value == "value1"));
        Assert.IsTrue(items.Any(item => item.Key == "key2" && item.Value == "value2"));
        Assert.IsTrue(items.Any(item => item.Key == "key3" && item.Value == "value3"));
    }

    private void ProduceMessage(string topic, string key, string value)
    {
        _inputTopic?.PipeInput(key, value);
    }
    
}