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
            string MetricKey(StreamMetric metric) => $"{metric.Group}_{metric.Name}".Replace("-", "_");
            
            var metrics = sensors.SelectMany(s => s.Metrics);
            foreach (var metric in metrics)
            {
                var metricKey = MetricKey(metric.Value);
                Gauge gauge = null;
                
                if (gauges.ContainsKey(metricKey))
                    gauge = gauges[metricKey];
                else
                {
                    gauge = global::Prometheus.Metrics.CreateGauge(metricKey, metric.Key.Description, 
                        new GaugeConfiguration {
                        LabelNames = metric.Key.Tags.Keys.ToArray()
                    });
                    
                    gauges.Add(metricKey, gauge);
                }

                if(IsNumeric(metric.Value.Value, out var value))
                    gauge.WithLabels(metric.Key.Tags.Values.ToArray()).Set(value);
                else
                    gauge.WithLabels(metric.Key.Tags.Values.ToArray()).Set(1);
            }
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