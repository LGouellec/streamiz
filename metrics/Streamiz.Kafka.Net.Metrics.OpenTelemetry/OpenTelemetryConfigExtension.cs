using System;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Metrics.OpenTelemetry
{
    public static class OpenTelemetryConfigExtension
    {
        private readonly static ILogger logger = Logger.GetLogger(typeof(OpenTelemetryConfigExtension));

        public static IStreamConfig UseOpenTelemetryReporter(
            this IStreamConfig config,
            TimeSpan metricInterval,
            Action<MeterProviderBuilder> actionMeterProviderBuilder = null,
            bool exposeLibrdkafkaStatistics = false)
        {
            var meterProviderBuilder = Sdk
                .CreateMeterProviderBuilder()
                .AddMeter("Streamiz")
                .SetResourceBuilder(
                    ResourceBuilder.CreateDefault()
                        .AddService(serviceName: "Streamiz"));
                
            actionMeterProviderBuilder?.Invoke(meterProviderBuilder);

            var port = RandomGenerator.GetInt32(10000);
            if (port < 5000)
                port += 5000;
            
            logger.LogInformation($"Open telemetry remote port is {port}");
            
            var tracerProvider = meterProviderBuilder.AddPrometheusExporter((options) =>
                {
                    // for test
                    options.StartHttpListener = true;
                    // Use your endpoint and port here
                    options.HttpListenerPrefixes = new string[] { $"http://localhost:{port}/" };
                    options.ScrapeResponseCacheDurationMilliseconds = 0;
                })
                .Build();
            
            var openTelemetryExporter = new OpenTelemetryMetricsExporter();
            var openTelemetryRunner = new OpenTelemetryRunner(tracerProvider, openTelemetryExporter);
            
            config.MetricsIntervalMs = (long) metricInterval.TotalMilliseconds;
            config.ExposeLibrdKafkaStats = exposeLibrdkafkaStatistics;
            config.MetricsReporter = openTelemetryExporter.ExposeMetrics;

            if (config.ExposeLibrdKafkaStats && config is StreamConfig streamConfig)
                streamConfig.StatisticsIntervalMs = (int) config.MetricsIntervalMs / 3;

            config.AddMiddleware(openTelemetryRunner);

            return config;
        }

        public static IStreamConfig UseOpenTelemetryReporter(
            this IStreamConfig config,
            Action<MeterProviderBuilder> actionMeterProviderBuilder = null,
            bool exposeLibrdkafkaStatistics = false)
            => UseOpenTelemetryReporter(config, TimeSpan.FromSeconds(30), actionMeterProviderBuilder,
                exposeLibrdkafkaStatistics);
    }
}