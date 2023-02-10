namespace Streamiz.Demo.KStream;

using Confluent.Kafka.Admin;
using Streamiz.Demo.Infrastructure;
using Streamiz.Demo.KStream.Models;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

public class WordSplitter : BackgroundService
{
    protected readonly ILogger<WordSplitter> logger;
    private readonly KStreamHelper helper;

    public WordSplitter(ILogger<WordSplitter> logger, StreamConfig config)
    {
        this.logger = logger;
        this.helper = new KStreamHelper(config);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await CreateTopics(("input", 1), ("output", 1));
        await helper.RunTopology(ConfigureTopology, stoppingToken);
    }

    private Task CreateTopics(params (string name, int partitions)[] topics) => helper.WithAdminClient(async client =>
    {
        try
        {
            await client.CreateTopicsAsync(topics.Select(t => new TopicSpecification { Name = t.name, NumPartitions = t.partitions }));
        }
        catch
        {
            logger.LogDebug("Could not create topics. assuming they already exist");
        }
    });

    public static void ConfigureTopology(StreamBuilder builder)
    {
        builder
            .Stream<string, string, StringSerDes, StringSerDes>("input")
            .MapValues(DemoValue.FromString)
            .To<StringSerDes, AvroSerDes<DemoValue>>("output");
    }
}
