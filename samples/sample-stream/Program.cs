using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var topicSource = "words";
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app-reproducer";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.StateDir = Path.Combine("./store");
            config.CommitIntervalMs = 5000;
            config.Logger = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddLog4Net();
            });
            
            using (IAdminClient client = new AdminClientBuilder(config.ToAdminConfig("topic-creator")).Build())
            {
                try
                {
                    TopicSpecification tp = new TopicSpecification()
                    {
                        Name = topicSource,
                        NumPartitions = 4
                    };
                    await client.CreateTopicsAsync(new List<TopicSpecification> {tp});
                }catch(Exception e){}
            }

            StreamBuilder builder = new StreamBuilder();
            builder
                .Stream<string, string>(topicSource)
                .FlatMapValues((k, v) => v.Split((" ")).ToList())
                .SelectKey((k, v) => v)
                .GroupByKey()
                .Count(RocksDb<string, long>.Create("count-store").WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes()));
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream.Dispose();

            await stream.StartAsync();
        }
    }
}