using Confluent.Kafka;
using System;
using System.Linq;
using System.Threading;

namespace assignor_tasks
{
    internal class Program
    {
        // NO WORK FOR MOMENT
        // https://github.com/confluentinc/confluent-kafka-dotnet/issues/1106
        // TODO : TEST THIS PR => https://github.com/confluentinc/confluent-kafka-dotnet/pull/1133
        private static void Main(string[] args)
        {
            var sourceToken = new CancellationTokenSource();
            var config = new ConsumerConfig()
            {
                BootstrapServers = "192.168.56.1:9092",
                GroupId = "test-group-assignorTask",
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "admin",
                SaslPassword = "admin",
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SessionTimeoutMs = 1000,
                MaxPollIntervalMs = 1000
            };
            var c1 = CreateConsumer(config, "client-1"); 
            var c2 = CreateConsumer(config, "client-2");
            var topic = "test";

            var wrappedC1 = new Consumer(c1, topic, 100, sourceToken.Token);
            var wrappedC2 = new Consumer(c2, topic, 100, sourceToken.Token);

            Console.CancelKeyPress += (o, e) =>
            {
                sourceToken.Cancel();
                wrappedC1.Dispose();
                wrappedC2.Dispose();
            };

            wrappedC1.Start();
            wrappedC2.Start();
        }

        static IConsumer<string, string> CreateConsumer(ConsumerConfig config, string instanceId)
        {
            var configuration = new ConsumerConfig(config);
            configuration.ClientId = instanceId;
            var builderC1 = new ConsumerBuilder<string, string>(configuration);
            builderC1.SetPartitionsRevokedHandler((c, p) =>
                {
                    string line = $"Consumer {c.MemberId} => Partitions Revoked {string.Join(",", p.Select(i => $"[{i.Topic}-{i.Partition}-{i.Offset}]"))}";
                    Console.WriteLine(line);
                })
                .SetPartitionsAssignedHandler((c, p) =>
                {
                    string line = $"Consumer {c.MemberId} => Partitions Assigned {string.Join(",", p.Select(i => $"[{i.Topic}-{i.Partition}]"))}";
                    Console.WriteLine(line);
                });
            return builderC1.Build();
        }
    }
}