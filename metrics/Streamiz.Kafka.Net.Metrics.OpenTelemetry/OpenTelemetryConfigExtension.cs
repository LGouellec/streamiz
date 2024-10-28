using System;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Streamiz.Kafka.Net.Metrics.OpenTelemetry
{
    public static class OpenTelemetryConfigExtension
    {
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

            meterProviderBuilder.AddOtlpExporter(options => {
                options.Protocol = OtlpExportProtocol.Grpc;
                options.ExportProcessorType = ExportProcessorType.Batch;
            });
            //meterProviderBuilder.AddPrometheusHttpListener();
            meterProviderBuilder.AddRuntimeInstrumentation();
            
            actionMeterProviderBuilder?.Invoke(meterProviderBuilder);

            var tracerProvider = meterProviderBuilder.Build();
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