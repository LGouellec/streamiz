using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream;

public class Reproducer314
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Hello Streams");

        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = $"test-windowedtable-bis",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var builder = CreateKVStore();
        var t = builder.Build();
        var windowedTableStream = new KafkaStream(t, config);

        Console.CancelKeyPress += (_,_) => {
            windowedTableStream.Dispose();
        };
        
        await windowedTableStream.StartAsync();

        // wait for the store to be restored and ready
        Thread.Sleep(10000);

        GetValueFromKVStore(windowedTableStream, DateTime.UtcNow.AddHours(-1), new CancellationToken());
        
        Console.WriteLine("Finished");
    }

    private static void GetValueFromKVStore(KafkaStream windowedTableStream, DateTime startUtcForWindowLookup, CancellationToken cancellationToken)
    {
        IReadOnlyKeyValueStore<string, int> windowedStore = null;
        while (windowedStore == null)
        {
            try
            {
                windowedStore =
                    windowedTableStream.Store(StoreQueryParameters.FromNameAndType("store",
                        QueryableStoreTypes.KeyValueStore<string, int>()));

            }
            catch (InvalidStateStoreException e)
            {
            }
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var records = windowedStore.All();

            if (records.Any())
            {
                foreach (var item in records)
                {
                    Console.WriteLine($"Value from windowed store : KEY = {item.Key} VALUE = {item.Value}");
                }
            }
        }
    }

    private static StreamBuilder CreateKVStore()
    {
        var builder = new StreamBuilder();

        builder
            .Stream<string, string>("users")
            .GroupByKey()
            .Aggregate(
                () => 0,
                (k, v, agg) => Math.Max(v.Length, agg),
                InMemory.As<string, int>("store")
                    .WithValueSerdes<Int32SerDes>()
                    .WithCachingEnabled());

        return builder;
    }
}