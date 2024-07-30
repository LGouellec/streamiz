using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
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

        var builder = CreateWindowedStore();
        var t = builder.Build();
        var windowedTableStream = new KafkaStream(t, config);

        await windowedTableStream.StartAsync();

        // wait for the store to be restored and ready
        Thread.Sleep(10000);

        GetValueFromWindowedStore(windowedTableStream, DateTime.UtcNow.AddHours(-1), new CancellationToken());

        Console.WriteLine("Finished");
    }

    private static void GetValueFromWindowedStore(KafkaStream windowedTableStream, DateTime startUtcForWindowLookup, CancellationToken cancellationToken)
    {
        var windowedStore = windowedTableStream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.WindowStore<string, int>()));

        while (!cancellationToken.IsCancellationRequested)
        {
            var records = windowedStore.FetchAll(startUtcForWindowLookup, DateTime.UtcNow).ToList();

            if (records.Count > 0)
            {
                foreach (var item in records)
                {
                    Console.WriteLine($"Value from windowed store : KEY = {item.Key} VALUE = {item.Value}");
                }

                startUtcForWindowLookup = DateTime.UtcNow;
            }
        }
    }

    private static StreamBuilder CreateWindowedStore()
    {
        var builder = new StreamBuilder();

        builder
            .Stream<string, string>("users")
            .GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(60000))
            .Aggregate(
                () => 0,
                (k, v, agg) => Math.Max(v.Length, agg),
                InMemoryWindows.As<string, int>("store").WithValueSerdes<Int32SerDes>());

        return builder;
    }
}