using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Streamiz.Kafka.Net.IntegrationTests.Fixtures;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.IntegrationTests;

public class RestoreStateTests
{
    private KafkaFixture kafkaFixture;
    
    private StreamBuilder _builder = new();
    private KafkaStream? _client;
    private IProducer<string, string> _producer = default!;
    
    private readonly string APPLICATION_ID = "state-restore-test";
    private readonly string STATE_STORE_NAME = "state";
    private readonly string INPUT_TOPIC = "events";
    private string OUTPUT_TOPIC => $"{APPLICATION_ID}-{STATE_STORE_NAME}-changelog";
    
    [SetUp]
    public void Setup()
    {
        kafkaFixture = new KafkaFixture();
        Console.WriteLine("Starting");
        kafkaFixture.InitializeAsync().Wait(TimeSpan.FromMinutes(5));
        Console.WriteLine("Started");
        
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = kafkaFixture.BootstrapServers,
        }).Build();
    }
        
    [TearDown]
    public void TearDown()
    {
        _producer.Dispose();
        _client?.Dispose();
        
        Console.WriteLine("Pending shutdown");
        kafkaFixture.DisposeAsync().Wait(TimeSpan.FromMinutes(5));
        Console.WriteLine("Shutdown");
    }

    [Test]
    [NonParallelizable]
    public async Task TestAtLeastOnceProcessing()
    {
        _builder = new();
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = APPLICATION_ID,
            BootstrapServers = kafkaFixture.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = true,
            Guarantee = ProcessingGuarantee.AT_LEAST_ONCE,
            Logger = LoggerFactory.Create(b =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddConsole();
            })
        };

        _builder.Stream<string, string>(INPUT_TOPIC)
            .Peek((k, v, c) => Console.WriteLine($"Event => Key: {k}, Value: {v}"))
            .GroupByKey()
            .Aggregate(() => "", (k, v, r) => v, InMemory.As<string, string>(STATE_STORE_NAME))
            .ToStream()
            .Peek((k, v, c) => Console.WriteLine($"State => Key: {k}, Value: {v}"));

        _client = new KafkaStream(_builder.Build(), config);

        await _client.StartAsync();

        ProduceMessage(INPUT_TOPIC, "key1", "value1");
        ProduceMessage(INPUT_TOPIC, "key2", "value2");
        ProduceMessage(INPUT_TOPIC, "key3", "value3");

        await Task.Delay(3000);

        var store = _client.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()));

        var items = store.All().ToList();
        Assert.IsNotEmpty(items);
        Assert.AreEqual(3, items.Count);

        await Task.Delay(3000);
        
        _client.Dispose();
        
        _client = new KafkaStream(_builder.Build(), config);
        
        await _client.StartAsync();
        
        await Task.Delay(3000);
        
        var store2 = _client.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()));
        
        var items2 = store2.All().ToList();
        Assert.IsNotEmpty(items2);
        Assert.AreEqual(3, items2.Count);
    }

    [Test, Timeout(15000)]
    [NonParallelizable]
    public async Task TestExactlyOnceProcessing()
    {
        _builder = new();
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = APPLICATION_ID,
            BootstrapServers = kafkaFixture.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = true,
            Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
            Logger = LoggerFactory.Create(b =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddConsole();
            })
        };

        _builder.Stream<string, string>(INPUT_TOPIC)
            .Peek((k, v, c) => Console.WriteLine($"Event => Key: {k}, Value: {v}"))
            .GroupByKey()
            .Aggregate(() => "", (k, v, r) => v, InMemory.As<string, string>(STATE_STORE_NAME))
            .ToStream()
            .Peek((k, v, c) => Console.WriteLine($"State => Key: {k}, Value: {v}"));

        _client = new KafkaStream(_builder.Build(), config);

        await _client.StartAsync();

        ProduceMessage(INPUT_TOPIC, "key1", "value1");
        ProduceMessage(INPUT_TOPIC, "key2", "value2");
        ProduceMessage(INPUT_TOPIC, "key3", "value3");

        await Task.Delay(3000);

        var store = _client.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()));

        var items = store.All().ToList();
        Assert.IsNotEmpty(items);
        Assert.AreEqual(3, items.Count);

        await Task.Delay(3000);
        
        _client.Dispose();
        
        _client = new KafkaStream(_builder.Build(), config);
        
        await _client.StartAsync();
        
        await Task.Delay(3000);
        
        var store2 = _client.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()));
        
        var items2 = store2.All().ToList();
        Assert.IsNotEmpty(items2);
        Assert.AreEqual(3, items2.Count);
    }
    
    private void ProduceMessage(string topic, string key, string value)
    {
        _producer.Produce(topic, new Message<string, string> { Key = key, Value = value }, result =>
        {
            Console.WriteLine($"Produce Result: {result.Status}");
        });
        _producer.Flush(TimeSpan.FromSeconds(10));
    }
}