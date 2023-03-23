using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    internal class PrometheusMetricsExporter
    {
        private readonly PrometheusRunner prometheusRunner;

        public PrometheusMetricsExporter(PrometheusRunner prometheusRunner)
        {
            this.prometheusRunner = prometheusRunner;
        }

        public void ExposeMetrics(IEnumerable<Sensor> sensors)
        {
            prometheusRunner.Expose(sensors);
        }
    }
}