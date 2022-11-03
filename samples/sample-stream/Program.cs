using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using System.Linq;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Stream;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        public static async Task Main(string[] args)
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app-reproducer";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 5000;
            config.Partitioner = Partitioner.Murmur2;
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddLog4Net();
            });
            
            StreamBuilder builder = new StreamBuilder();
            var stream1 = builder.Stream<string, string>("topic1");
            var stream2 = builder.Stream<string, string>("topic2");
                
            stream1.Join(stream2, 
                (v1,v2) => $"{v1}-{v2}",
                JoinWindowOptions.Of(TimeSpan.FromHours(1)))
                .To("topic3");
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream.Dispose();
            
            await stream.StartAsync();
        }
    }
}