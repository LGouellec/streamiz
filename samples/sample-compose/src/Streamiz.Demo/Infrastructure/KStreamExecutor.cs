namespace Streamiz.Demo.Infrastructure;

using Confluent.Kafka;
using Streamiz.Kafka.Net;

public class KStreamHelper
{
    private readonly StreamConfig config;

    public KStreamHelper(StreamConfig config)
    {
        this.config = config;
    }

    public async Task WithAdminClient(Func<IAdminClient, Task> action)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = config.BootstrapServers };
        using var client = new AdminClientBuilder(adminConfig).Build();
        await action(client);
    }

    public async Task RunTopology(Action<StreamBuilder> configurator, CancellationToken stoppingToken)
    {
        StreamBuilder builder = new();
        configurator(builder);

        // this stream will be disposed, when tcs.Task finishes
        using KafkaStream stream = new KafkaStream(builder.Build(), config);

        // this operation does not block. Just starts the processors
        await stream.StartAsync();

        var tcs = new TaskCompletionSource();
        stoppingToken.Register(() => tcs.SetResult());
        await tcs.Task;
    }
}
