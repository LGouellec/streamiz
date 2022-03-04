using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    public class PrometheusMetricsExporter
    {
        private readonly PrometheusRunner prometheusRunner;

        public PrometheusMetricsExporter(PrometheusRunner prometheusRunner)
        {
            this.prometheusRunner = prometheusRunner;
        }

        public void ExposeMetrics(IEnumerable<Sensor> sensors)
        {
            // TODO : 
        }
    }
}