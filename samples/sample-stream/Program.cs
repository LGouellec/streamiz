using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Security.Permissions;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes.CloudEvents;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        class Order
        {
            public int OrderId { get; set; }
            public string ProductId { get; set; }
            public DateTime OrderTime { get; set; }
        }
        
        
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig();
            config.ApplicationId = "test-app-cloud-events";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 3000;
            config.StateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Information);
                b.AddLog4Net();
            });
            
            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, Order, StringSerDes, JsonSerDes<Order>>("order")
                .Filter((k, v) => v.OrderId >= 200)
                .To<StringSerDes, CloudEventSerDes<Order>>("order-filtered");

            Topology t = builder.Build();
            KafkaStream stream1 = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream1.Dispose();
            
            await stream1.StartAsync();
        }
    }
}