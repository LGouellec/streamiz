using System;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    /// <summary>
    /// Extensions class to configure the prometheus reporter
    /// </summary>
    public static class PrometheusConfigExtension
    {
        /// <summary>
        /// This extension method configures a Prometheus metrics reporter for a stream configuration.
        /// It integrates Prometheus-based monitoring into the stream processing system, allowing the application to expose metrics at a specific endpoint.
        /// Additionally, it provides options to customize the metrics collection interval and expose statistics related to librdkafka.
        /// </summary>
        /// <param name="config">The stream configuration object to which the Prometheus metrics reporter will be added.</param>
        /// <param name="metricInterval">The time interval (in milliseconds) at which metrics will be collected and reported. This value is used to set the <see cref="IStreamConfig.MetricsIntervalMs"/> property of the configuration.</param>
        /// <param name="prometheusReporterEndpointPort">The port on which the Prometheus exporter will expose metrics. Default value is 9090.</param>
        /// <param name="exposeLibrdkafkaStatistics">A flag indicating whether librdkafka statistics should be exposed. If true, additional configuration is applied to collect and expose these statistics.</param>
        /// <returns>The configured stream instance (config) with the Prometheus reporter integrated.</returns>
        public static IStreamConfig UsePrometheusReporter(
            this IStreamConfig config, 
            TimeSpan metricInterval,
            int prometheusReporterEndpointPort = 9090,
            bool exposeLibrdkafkaStatistics = false)
        {
            var prometheusRunner = new PrometheusRunner(prometheusReporterEndpointPort);
            var prometheusMetricsExporter = new PrometheusMetricsExporter(prometheusRunner); 
            
            config.MetricsIntervalMs = (long) metricInterval.TotalMilliseconds;
            config.ExposeLibrdKafkaStats = exposeLibrdkafkaStatistics;
            config.MetricsReporter = prometheusMetricsExporter.ExposeMetrics;
            config.AddMiddleware(prometheusRunner);

            if (config.ExposeLibrdKafkaStats && config is StreamConfig streamConfig)
                streamConfig.StatisticsIntervalMs = (int) config.MetricsIntervalMs / 3;
            
            return config;
        }

        /// <summary>
        /// This extension method configures a Prometheus metrics reporter for a stream configuration.
        /// It integrates Prometheus-based monitoring into the stream processing system, allowing the application to expose metrics at a specific endpoint.
        /// Additionally, it provides options to customize the metrics collection interval and expose statistics related to librdkafka.
        /// </summary>
        /// <param name="config">The stream configuration object to which the Prometheus metrics reporter will be added.</param>
        /// <param name="prometheusReporterEndpointPort">The port on which the Prometheus exporter will expose metrics. Default value is 9090.</param>
        /// <param name="exposeLibrdkafkaStatistics">A flag indicating whether librdkafka statistics should be exposed. If true, additional configuration is applied to collect and expose these statistics.</param>
        /// <returns>The configured stream instance (config) with the Prometheus reporter integrated.</returns>
        public static IStreamConfig UsePrometheusReporter(
            this IStreamConfig config,
            int prometheusReporterEndpointPort,
            bool exposeLibrdkafkaStatistics = false)
            => UsePrometheusReporter(config, TimeSpan.FromSeconds(30), prometheusReporterEndpointPort, exposeLibrdkafkaStatistics);
    }
}