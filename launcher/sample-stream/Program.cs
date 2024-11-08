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
        public static void Main2()
        {
            Meter? meter = null;

            var meterProviderBuilder = Sdk
                .CreateMeterProviderBuilder()
                .AddMeter("Test")
                .SetResourceBuilder(
                    ResourceBuilder.CreateDefault()
                        .AddService(serviceName: "Test"));

            meterProviderBuilder.AddOtlpExporter((exporterOptions, readerOptions) =>
            {
                readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 5000;
            });

            using var meterProvider = meterProviderBuilder.Build();

            while (true)
            {
                meter?.Dispose();
                GC.Collect();
                meter = new Meter("Test");
                var rd = new Random();

                meter.CreateObservableGauge(
                    "requests",
                    () => new[]
                    {
                        new Measurement<double>(rd.NextDouble() * rd.Next(100)),
                    },
                    description: "Request per second");

                // will see after couple of minutes that the MetricReader contains a lot of MetricPoint[], even if we dispose the Meter after each iteration
                Thread.Sleep(200);
            }
        }
        
       public static async Task Main(string[] args)
       {
           var config = new StreamConfig<StringSerDes, StringSerDes>{
                ApplicationId = $"test-app",
                BootstrapServers = "pkc-p11xm.us-east-1.aws.confluent.cloud:9092",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "VYBKPVLBPW2CYRX4",
                CommitIntervalMs = 200,
                SaslPassword = "FdtYt9HdVQUo0RkI5tIrdvbfdYm3BjJfKjNiYZGBfb1VHcOABtsZR1P7ib6DKB6p",
                SessionTimeoutMs = 45000,
                ClientId = "ccloud-csharp-client-f7bb4f5b-f37d-4956-851e-e106065963b8",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                MetricsRecording = MetricsRecordingLevel.DEBUG,
                MetricsIntervalMs = 500,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Information);
                })
            };

           config.UseOpenTelemetryReporter();
           //config.UsePrometheusReporter(9091);
           
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

            builder.Stream<string, string>("sample_data")
                .Print(Printed<string, string>.ToOut());
                //.To("output2");
                
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
