using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;

namespace Streamiz.Kafka.Net.Metrics.OpenTelemetry
{
    public class OpenTelemetryMetricsExporter
    {
        private Meter meter;
        private readonly IDictionary<string, ObservableGauge<double>> gauges = new Dictionary<string, ObservableGauge<double>>();
        
        public OpenTelemetryMetricsExporter()
        {
        }
        
        public void ExposeMetrics(IEnumerable<Sensor> sensors)
        {
            meter?.Dispose();
            meter = new Meter("Streamiz");
            
            string MetricKey(StreamMetric metric) => $"{metric.Group}_{metric.Name}".Replace("-", "_");
            
            var metrics = sensors.SelectMany(s => s.Metrics);
            foreach (var metric in metrics)
            {
                var metricKey = MetricKey(metric.Value);
                var realMetricKey =
                    $"{metricKey}_{string.Join("_", metric.Key.Tags.Select(kv => $"{kv.Key}={kv.Value}"))}";

                // if(!gauges.ContainsKey(realMetricKey))
               // {
                    meter.CreateObservableGauge(
                        metricKey, 
                        () => new[]
                        {
                            new Measurement<double>(
                                IsNumeric(metric.Value.Value, out var value) ? value : 1d,
                                metric.Key.Tags.Select(kv => new KeyValuePair<string,object>(kv.Key, kv.Value)).ToArray())
                        },
                        description: metric.Key.Description);
                    
                 //   gauges.Add(realMetricKey, gauge);
               // }
            }
        }
        
        private bool IsNumeric(object expression, out Double number)
        {
            return Double.TryParse( Convert.ToString( expression
                    , CultureInfo.InvariantCulture)
                , System.Globalization.NumberStyles.Any
                , NumberFormatInfo.InvariantInfo
                , out number);
        }

        public void Dispose()
        {
            meter.Dispose();
        }
    }
}