using Streamiz.Kafka.Net;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    public static class Program
    {
       public static async Task Main(string[] args)
       {
           var config = new StreamConfig<StringSerDes, StringSerDes>{
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Debug);
                })
            };
           
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

            builder.Stream<string, string>("input2")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .Count()
                .Suppress(SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded())
                    .WithKeySerdes(new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds)))
                .ToStream()
                .Map((k,v, r) => new KeyValuePair<string,long>(k.Key, v))
                .To<StringSerDes, Int64SerDes>("output2");
            
            return builder.Build();
        }
    }
}
