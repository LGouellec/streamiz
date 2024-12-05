using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;

namespace Streamiz.Kafka.Net.Metrics.OpenTelemetry.Internal
{
    /// <summary>
    /// Export the metrics using an instance of <see cref="Meter"/>
    /// </summary>
    internal class OpenTelemetryMetricsExporter
    {
        private Meter meter;

        private readonly IDictionary<string, ObservableGauge<double>> gauges =
            new Dictionary<string, ObservableGauge<double>>();

        /// <summary>
        /// Expose the current sensors/metrics
        /// </summary>
        /// <param name="sensors">Sensors to emit</param>
        public void ExposeMetrics(IEnumerable<Sensor> sensors)
        {
            GC.Collect();
            meter?.Dispose();
            meter = new Meter("Streamiz");

            string MetricKey(StreamMetric metric) => $"{metric.Group}_{metric.Name}".Replace("-", "_");

            var metrics = sensors.SelectMany(s => s.Metrics);
            foreach (var metric in metrics)
            {
                var metricKey = MetricKey(metric.Value);
                meter.CreateObservableGauge(
                    metricKey,
                    () => new[]
                    {
                        new Measurement<double>(
                            Crosscutting.Utils.IsNumeric(metric.Value.Value, out var value) ? value : 1d,
                            metric.Key.Tags.Select(kv => new KeyValuePair<string, object>(kv.Key, kv.Value)).ToArray())
                    },
                    description: metric.Key.Description);
            }
        }

        public void Dispose()
        {
            meter?.Dispose();
        }
    }
}