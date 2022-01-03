using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_micro_service.Service
{
    public class StreamizService : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            config.StateDir = Path.Combine(".", Guid.NewGuid().ToString());
            
            StreamBuilder builder = new StreamBuilder();

            builder
                .Table("table", RocksDb<string, string>.As("table-store").WithLoggingEnabled())
                .ToStream()
                .Print(Printed<string, string>.ToOut());

            Topology t = builder.Build();
            
            KafkaStream stream = new KafkaStream(t, config);
            await stream.StartAsync(stoppingToken);
        }
    }
}