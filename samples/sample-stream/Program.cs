using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Runtime.Intrinsics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
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
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Logger = LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(LogLevel.Debug);
                    b.AddConsole();
                }),
                Debug = "all"
            };
            
            var builder = new StreamBuilder();

            var inputStream = builder.Stream<string, string>("input");
            
            inputStream
                .FlatMapValuesAsync(async (record, _) => await HandleEvent(record.Value))
                .To("output");


            var t = builder.Build();
            var stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) =>
            {
                stream.Dispose();
            };
            
            await stream.StartAsync();
        }

        private static async Task<IEnumerable<string>> HandleEvent(string recordValue)
        {
            return await Task.FromResult(recordValue.Split(" "));
        }
    }
}