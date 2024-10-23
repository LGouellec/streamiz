using Streamiz.Kafka.Net;
using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using Streamiz.Kafka.Net.Metrics.OpenTelemetry;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;

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
                MetricsRecording = MetricsRecordingLevel.DEBUG,
                MetricsIntervalMs = 500,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Debug);
                })
            };

           config.UseOpenTelemetryReporter();
           //config.UsePrometheusReporter(9090);
           
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

            builder.Stream<string, string>("input")
                .To("output2");
                
                /*.GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .Count()
                .Suppress(SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded())
                    .WithKeySerdes(new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds)))
                .ToStream()
                .Map((k,v, r) => new KeyValuePair<string,long>(k.Key, v))
                .To<StringSerDes, Int64SerDes>("output");*/
            
            return builder.Build();
        }
    }
}
