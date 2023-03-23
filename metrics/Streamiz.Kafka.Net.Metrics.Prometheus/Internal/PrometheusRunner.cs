using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    internal class PrometheusRunner : IStreamMiddleware
    {
        private PrometheusMetricServer MetricsServer { get; }

        public PrometheusRunner(int prometheusExporterEndpointPort)
        {
            MetricsServer = new PrometheusMetricServer(prometheusExporterEndpointPort);
        }

        #region IStreamMiddleware impl
        
        public void BeforeStart(IStreamConfig config, CancellationToken token)
        {
            MetricsServer.Start(token);
        }

        public void AfterStart(IStreamConfig config, CancellationToken token)
        {
            // nothing
        }

        public void BeforeStop(IStreamConfig config, CancellationToken token)
        {
            // nothing 
        }

        public void AfterStop(IStreamConfig config, CancellationToken token)
        {
            MetricsServer.Stop();
        }
        
        #endregion
        
        internal void Expose(IEnumerable<Sensor> sensors)
        {
            string MetricKey(StreamMetric metric) => $"{metric.Group}_{metric.Name}".Replace("-", "_");
            
            MetricsServer.ClearGauges();

            foreach (var metric in sensors.SelectMany(s => s.Metrics))
            {
                var metricKey = MetricKey(metric.Value);
                var newGauge = new Gauge(metricKey, metric.Key.Description, metric.Value.Tags);
                MetricsServer.AddGauge(newGauge, metric);
            }
        }
    }
}