using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;

namespace Streamiz.Kafka.Net.Metrics.OpenTelemetry
{
    public class OpenTelemetryMetricsExporter
    {
        private readonly Meter meter;
        private readonly IDictionary<string, ObservableGauge<double>> gauges = new Dictionary<string, ObservableGauge<double>>();
        
        public OpenTelemetryMetricsExporter()
        {
            meter = new Meter("Streamiz");
        }
        
        public void ExposeMetrics(IEnumerable<Sensor> sensors)
        {
            string MetricKey(StreamMetric metric) => $"{metric.Group}_{metric.Name}".Replace("-", "_");
            
            var metrics = sensors.SelectMany(s => s.Metrics);
            foreach (var metric in metrics)
            {
                var metricKey = MetricKey(metric.Value);
                ObservableGauge<double> gauge;
                
                if(!gauges.ContainsKey(metricKey))
                {
                    gauge = meter.CreateObservableGauge(
                        metricKey, 
                        () => new[]
                        {
                            new Measurement<double>(
                                IsNumeric(metric.Value.Value, out var value) ? value : 1d,
                                metric.Key.Tags.Select(kv => new KeyValuePair<string,object>(kv.Key, kv.Value)).ToArray())
                        },
                        description: metric.Key.Description);
                    
                    gauges.Add(metricKey, gauge);
                }
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