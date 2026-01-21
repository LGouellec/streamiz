using System;
using System.Collections.Generic;
using System.Reflection.Metadata;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bogus;
using Confluent.Kafka;
using ICSharpCode.SharpZipLib.Tar;
using NUnit.Framework;
using Streamiz.Kafka.Net.IntegrationTests.Fixtures;
using Streamiz.Kafka.Net.IntegrationTests.Seed;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.IntegrationTests;

public sealed class LargeConcurrencyTests
{
    private class KeySeeder : ISeeder<string>
    {
        private readonly Faker faker;

        public KeySeeder()
        {
            faker = new Faker();
        }

        public string SeedOnce()
            => $"sess_{faker.Random.AlphaNumeric(2)}";

    }

    private class SessionSeeder : ISeeder<SessionState>
    {
        private readonly Faker<SessionState> faker;

        public SessionSeeder()
        {
            faker = new Faker<SessionState>("en")
                .RuleFor(s => s.Id, f => f.Random.Guid().ToString())
                .RuleFor(s => s.Date, f => f.Date.Recent(30))
                .RuleFor(s => s.SessionId, f => $"sess_{f.Random.AlphaNumeric(2)}")
                .RuleFor(s => s.User, f => f.Internet.UserName());
        }
        
        public SessionState SeedOnce()
            => faker.Generate();
    }
    
    private class SessionProcessor : ITransformer<string, SessionState, string, SessionState>
    {
        private IKeyValueStore<string, SessionState> store1;
        private IKeyValueStore<string, byte[]> store2;
        
        public void Init(ProcessorContext<string, SessionState> context)
        {
            store1 = (IKeyValueStore<string, SessionState>)context.GetStateStore("store1");
            store2 = (IKeyValueStore<string, byte[]>)context.GetStateStore("store2");
        }

        public Record<string, SessionState> Process(Record<string, SessionState> record)
        {
            store1.Put(record.Key, record.Value);
            store2.Put(record.Key, Encoding.UTF8.GetBytes(record.Value.User));
            return record;
        }

        public void Close()
        {
           
        }
    }
    
    private class SessionState
    {
        public string Id { get; set; }
        public DateTime Date { get; set; }
        public string SessionId { get; set; }
        public string User { get; set; }
    }
    
    private KafkaFixture kafkaFixture;
    
    [SetUp]
    public void Setup()
    {
        kafkaFixture = new KafkaFixture();
        Console.WriteLine("Starting");
        kafkaFixture.InitializeAsync().Wait(TimeSpan.FromMinutes(5));
        Console.WriteLine("Started");
    }
        
    [TearDown]
    public void TearDown()
    {
        Console.WriteLine("Pending shutdown");
        kafkaFixture.DisposeAsync().Wait(TimeSpan.FromMinutes(5));
        Console.WriteLine("Shutdown");
    }
    
    [Test]
    [NonParallelizable]
    public async Task TestSimpleTopology()
    {
        var tokenSource = new CancellationTokenSource();
        var config = new StreamConfig<StringSerDes, JsonSerDes<SessionState>>
        {
            ApplicationId = $"test-concurrency-{Guid.NewGuid().ToString()}",
            BootstrapServers = kafkaFixture.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            NumStreamThreads = Math.Min(Environment.ProcessorCount, 4),
            Guarantee = ProcessingGuarantee.AT_LEAST_ONCE,
            CommitIntervalMs = StreamConfig.EOS_DEFAULT_COMMIT_INTERVAL_MS * 10 * 5, // 5 seconds
            AllowAutoCreateTopics = false,
            CompressionType = CompressionType.Lz4
        };
        
        await kafkaFixture.CreateTopic("source", 30);
        await kafkaFixture.CreateTopic("output", 30);

        var builder = new StreamBuilder();

        var sourceStream = builder
            .Stream<string, SessionState>("source");
        
        var store1Builder = Stores.KeyValueStoreBuilder(
                Stores.PersistentKeyValueStore("store1"),
                new StringSerDes(),
                new JsonSerDes<SessionState>()) 
            .WithLoggingEnabled(new Dictionary<string, string>()) // Changelog for EOS
            .WithCachingEnabled(); // Reduce write amplification

        builder.AddStateStore(store1Builder);

        var store2Builder = Stores.KeyValueStoreBuilder(
                Stores.PersistentKeyValueStore("store2"),
                new StringSerDes(),
                new ByteArraySerDes())
            .WithLoggingEnabled(new Dictionary<string, string>());

        builder.AddStateStore(store2Builder);

        sourceStream.Transform(
                TransformerBuilder.New<string, SessionState, string, SessionState>()
                    .Transformer<SessionProcessor>()
                    .Build(), null, "store1", "store2")
            .To<StringSerDes, JsonSerDes<SessionState>>("output");

        KafkaStream.State stream1State = KafkaStream.State.CREATED, stream2State = KafkaStream.State.CREATED;
        var stream1 = new KafkaStream(builder.Build(), config);
        stream1.StateChanged += (_old, _new) => stream1State = _new;
        var stream2 = new KafkaStream(builder.Build(), config);
        stream1.StateChanged += (_old, _new) => stream2State = _new;

        var stats = new KafkaStats();
        var task = kafkaFixture.ProduceContinuously(
            "source",
            new KeySeeder(),
            new SessionSeeder(),
            (key, session) => session.SessionId = key,
            new StringSerDes(),
            new JsonSerDes<SessionState>(),
            TimeSpan.FromMilliseconds(1),
            stats,
            tokenSource.Token);
            
        await stream1.StartAsync();
        Thread.Sleep(TimeSpan.FromSeconds(4));
        
        await stream2.StartAsync();
        Thread.Sleep(TimeSpan.FromSeconds(4));

        stream2.Dispose();
        Thread.Sleep(TimeSpan.FromSeconds(4));

        Assert.AreNotEqual(KafkaStream.State.ERROR ,stream1State);
        Assert.AreNotEqual(KafkaStream.State.ERROR ,stream2State);
        
        await tokenSource.CancelAsync();
        await task;
        
        var result = kafkaFixture.ConsumeUntil("output", stats.MessagesCorrectlyPersisted, 1000 * 120);
            
        stream1.Dispose();
        stream2.Dispose();
        
        Assert.IsTrue(result);
    }
}