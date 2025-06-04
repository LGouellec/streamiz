using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.IntegrationTests;

using State;
using Table;
using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Fixtures;
using SerDes;
using NUnit.Framework;

public class GlobalRestoreStateTests
{
    private KafkaFixture kafkaFixture;
    
    private StreamBuilder _builder = new();
    private StreamBuilder _globalBuilder = new();
    private KafkaStream? _client;
    private KafkaStream? _globalClient;
    private IProducer<string, string> _producer = default!;
    
    private readonly string APPLICATION_ID = "global-restore-test";
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
        _globalClient?.Dispose();
        
        Console.WriteLine("Pending shutdown");
        kafkaFixture.DisposeAsync().Wait(TimeSpan.FromMinutes(5));
        Console.WriteLine("Shutdown");
    }

    [Test]
    [NonParallelizable]
    public async Task TestAtLeastOnceProcessing()
    {
        _builder = new();
        _globalBuilder = new();
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = APPLICATION_ID,
            BootstrapServers = kafkaFixture.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = true,
            Guarantee = ProcessingGuarantee.AT_LEAST_ONCE,
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

        await Task.Delay(3000);

        _globalBuilder.GlobalTable(OUTPUT_TOPIC, InMemory.As<string, string>(STATE_STORE_NAME));
        _globalClient = new KafkaStream(_globalBuilder.Build(), config);
        
        await _globalClient.StartAsync();

        await Task.Delay(3000);

        var store = _globalClient.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()
            )
        );

        var items = store.All();

        Assert.IsNotEmpty(items);
        Assert.AreEqual("key1", items.First().Key);
        Assert.AreEqual("value1", items.First().Value);
    }

    [Test, Timeout(15000)]
    [NonParallelizable]
    public async Task TestExactlyOnceProcessing()
    {
        _builder = new();
        _globalBuilder = new();
        
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

        await Task.Delay(3000);

        _globalBuilder.GlobalTable(OUTPUT_TOPIC, InMemory.As<string, string>(STATE_STORE_NAME));
        _globalClient = new KafkaStream(_globalBuilder.Build(), config);
        await _globalClient.StartAsync();

        await Task.Delay(3000);

        var store = _globalClient.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()
            )
        );

        var items = store.All();

        Assert.IsNotEmpty(items);
        Assert.AreEqual("key1", items.First().Key);
        Assert.AreEqual("value1", items.First().Value);
    }
    
    [Test, Timeout(30000)]
    [NonParallelizable]
    public async Task TestExactlyOnceProcessingWithPersistentStorageDisk()
    {
        _builder = new();
        _globalBuilder = new();
        
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = APPLICATION_ID,
            BootstrapServers = kafkaFixture.BootstrapServers,
            StateDir = Path.Combine(Path.GetTempPath(), "global-restore-rocksb"), 
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

        await Task.Delay(3000);

        _globalBuilder.GlobalTable(OUTPUT_TOPIC, 
            RocksDb.As<string, string>(STATE_STORE_NAME));
        var globalTopo = _globalBuilder.Build();
        _globalClient = new KafkaStream(globalTopo, config);
        await _globalClient.StartAsync();

        await Task.Delay(3000);

        var store = _globalClient.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()
            )
        );

        var items = store.All();

        Assert.IsNotEmpty(items);
        Assert.AreEqual("key1", items.First().Key);
        Assert.AreEqual("value1", items.First().Value);
        
        _globalClient.Dispose();
        
        ProduceMessage(INPUT_TOPIC, "key2", "value2");
        ProduceMessage(INPUT_TOPIC, "key3", "value3");
        
        await Task.Delay(1000);

        _globalBuilder = new();
        _globalBuilder.GlobalTable(OUTPUT_TOPIC, 
            RocksDb.As<string, string>(STATE_STORE_NAME));
        _globalClient = new KafkaStream(_globalBuilder.Build(), config);
        await _globalClient.StartAsync();
        
        await Task.Delay(3000);
            
        store = _globalClient.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()
            )
        );

        var itemsList = store.All().ToList();

        Assert.IsNotEmpty(itemsList);
        Assert.AreEqual(3, itemsList.Count());
        Assert.AreEqual("key1", itemsList[0].Key);
        Assert.AreEqual("value1", itemsList[0].Value);
        Assert.AreEqual("key2", itemsList[1].Key);
        Assert.AreEqual("value2", itemsList[1].Value);
        Assert.AreEqual("key3", itemsList[2].Key);
        Assert.AreEqual("value3", itemsList[2].Value);
        
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