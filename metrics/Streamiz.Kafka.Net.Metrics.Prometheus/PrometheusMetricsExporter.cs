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
        private readonly IDictionary<string, Gauge> gauges = new Dictionary<string, Gauge>();

        public PrometheusMetricsExporter(PrometheusRunner prometheusRunner)
        {
            this.prometheusRunner = prometheusRunner;
        }

        public void ExposeMetrics(IEnumerable<Sensor> sensors)
        {
            prometheusRunner.Expose(sensors);
        }

        private bool IsNumeric(object expression, out Double number)
        {
            if (expression == null)
            {
                number = Double.NaN;
                return false;
            }

            return Double.TryParse( Convert.ToString( expression
                    , CultureInfo.InvariantCulture)
                , System.Globalization.NumberStyles.Any
                , NumberFormatInfo.InvariantInfo
                , out number);
        }
    }
}