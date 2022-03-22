using System;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    public static class PrometheusConfigExtension
    {
        public static IStreamConfig UsePrometheusExporter(
            this IStreamConfig config, 
            TimeSpan metricInterval,
            int prometheusExporterEndpointPort = 9090,
            bool exposeLibrdkafkaStatistics = false)
        {
            var prometheusRunner = new PrometheusRunner(prometheusExporterEndpointPort);
            var prometheusMetricsExporter = new PrometheusMetricsExporter(prometheusRunner); 
            
            config.MetricsIntervalMs = (long) metricInterval.TotalMilliseconds;
            config.ExposeLibrdKafkaStats = exposeLibrdkafkaStatistics;
            config.MetricsReporter = prometheusMetricsExporter.ExposeMetrics;
            config.Add(prometheusRunner);
            
            return config;
        }

        public static IStreamConfig UsePrometheusExporter(
            this IStreamConfig config,
            int prometheusExporterEndpointPort,
            bool exposeLibrdkafkaStatistics = false)
            => UsePrometheusExporter(config, TimeSpan.FromSeconds(30), prometheusExporterEndpointPort, exposeLibrdkafkaStatistics);
    }
}