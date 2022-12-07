using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Prometheus;

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
            prometheusRunner.Expose(sensors);
        }
    }
}