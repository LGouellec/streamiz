using System;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Streamiz.Kafka.Net.Metrics.OpenTelemetry.Internal;

namespace Streamiz.Kafka.Net.Metrics.OpenTelemetry
{
    /// <summary>
    /// Extensions class to configure the open telemetry reporter
    /// </summary>
    public static class OpenTelemetryConfigExtension
    {
        /// <summary>
        /// This extension method configures an OpenTelemetry metrics reporter for a stream configuration.
        /// It allows you to monitor and export application metrics, integrating OpenTelemetry with the stream processing system.
        /// The method provides a customizable way to set up metric collection intervals, configure the OpenTelemetry MeterProvider,
        /// and optionally expose statistics related to the underlying librdkafka library.
        /// </summary>
        /// <param name="config">The stream configuration object to which the OpenTelemetry metrics reporter will be added</param>
        /// <param name="metricInterval">The time interval (in milliseconds) at which metrics will be collected and reported. This is used to set the <see cref="IStreamConfig.MetricsIntervalMs"/> property of the configuration.</param>
        /// <param name="actionMeterProviderBuilder">A delegate allowing additional customization of the MeterProviderBuilder. If not provided, the default setup is used.</param>
        /// <param name="exposeLibrdkafkaStatistics">Whether to expose statistics related to librdkafka. If set to true, additional configuration is applied to collect and export librdkafka statistics.</param>
        /// <returns>The configured stream instance (config) with the OpenTelemetry reporter integrated.</returns>
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
            meterProviderBuilder.AddRuntimeInstrumentation();
            //meterProviderBuilder.AddConsoleExporter();
            
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

        /// <summary>
        /// This extension method configures an OpenTelemetry metrics reporter for a stream configuration.
        /// It allows you to monitor and export application metrics, integrating OpenTelemetry with the stream processing system.
        /// The method provides a customizable way to set up metric collection intervals, configure the OpenTelemetry MeterProvider,
        /// and optionally expose statistics related to the underlying librdkafka library.
        /// </summary>
        /// <param name="config">The stream configuration object to which the OpenTelemetry metrics reporter will be added</param>
        /// <param name="actionMeterProviderBuilder">A delegate allowing additional customization of the MeterProviderBuilder. If not provided, the default setup is used.</param>
        /// <param name="exposeLibrdkafkaStatistics">Whether to expose statistics related to librdkafka. If set to true, additional configuration is applied to collect and export librdkafka statistics.</param>
        /// <returns>The configured stream instance (config) with the OpenTelemetry reporter integrated.</returns>
        public static IStreamConfig UseOpenTelemetryReporter(
            this IStreamConfig config,
            Action<MeterProviderBuilder> actionMeterProviderBuilder = null,
            bool exposeLibrdkafkaStatistics = false)
            => UseOpenTelemetryReporter(config, TimeSpan.FromSeconds(30), actionMeterProviderBuilder,
                exposeLibrdkafkaStatistics);
    }
}