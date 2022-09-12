using System.Collections.Generic;
using Prometheus;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    public class PrometheusMetricsExporter
    {
        private readonly PrometheusRunner prometheusRunner;
        private readonly IDictionary<string, Gauge> gauges = new Dictionary<string, Gauge>();

        public PrometheusMetricsExporter(PrometheusRunner prometheusRunner)
        {
            this.prometheusRunner = prometheusRunner;
        }

        public void ExposeMetrics(IEnumerable<Sensor> sensors)
            => prometheusRunner.Expose(sensors);
    }
}