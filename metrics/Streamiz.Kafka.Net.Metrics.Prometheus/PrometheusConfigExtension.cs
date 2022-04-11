using System;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    public static class PrometheusConfigExtension
    {
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
            config.Add(prometheusRunner);

            if (config.ExposeLibrdKafkaStats && config is StreamConfig streamConfig)
                streamConfig.StatisticsIntervalMs = (int) config.MetricsIntervalMs / 3;
            
            return config;
        }

        public static IStreamConfig UsePrometheusReporter(
            this IStreamConfig config,
            int prometheusReporterEndpointPort,
            bool exposeLibrdkafkaStatistics = false)
            => UsePrometheusReporter(config, TimeSpan.FromSeconds(30), prometheusReporterEndpointPort, exposeLibrdkafkaStatistics);
    }
}