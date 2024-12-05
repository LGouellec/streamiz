using Streamiz.Kafka.Net;
using System;
using System.Diagnostics.Metrics;
using System.Collections.Generic;
using System.Reflection;
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
                BootstrapServers = "",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "",
                CommitIntervalMs = 200,
                SaslPassword = "",
                SessionTimeoutMs = 45000,
                ClientId = "ccloud-csharp-client-f7bb4f5b-f37d-4956-851e-e106065963b8",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                MetricsRecording = MetricsRecordingLevel.DEBUG,
                MetricsIntervalMs = 500,
                Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Information);
                }),
                NumStreamThreads = 2
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
            
            return builder.Build();
        }
    }
}
