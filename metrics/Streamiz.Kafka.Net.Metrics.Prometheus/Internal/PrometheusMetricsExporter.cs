using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    /// <summary>
    /// Export the metrics through the prometheus exporter endpoint
    /// </summary>
    internal class PrometheusMetricsExporter
    {
        private readonly PrometheusRunner prometheusRunner;

        public PrometheusMetricsExporter(PrometheusRunner prometheusRunner)
        {
            this.prometheusRunner = prometheusRunner;
        }

        /// <summary>
        /// Expose the current sensors/metrics
        /// </summary>
        /// <param name="sensors">Sensors to emit</param>
        public void ExposeMetrics(IEnumerable<Sensor> sensors)
        {
            prometheusRunner.Expose(sensors);
        }
    }
}