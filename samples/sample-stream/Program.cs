using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Stream;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            config.UsePrometheusReporter(9090, true);

            var t = BuildTopology();
            var stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (_,_) => {
                stream.Dispose();
            };

            await stream.StartAsync();
        }
        
        private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();
            
            var globalTable = builder
                .GlobalTable("Input2", 
                    new StringSerDes(),
                    new StringSerDes(), 
                    InMemory.As<String, String>()
                        .WithKeySerdes<StringSerDes>()
                        .WithValueSerdes<StringSerDes>());
          
          return builder.Build();
        }
    }
}