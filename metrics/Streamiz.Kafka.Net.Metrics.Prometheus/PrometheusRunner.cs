using System.Collections.Generic;
using System.Linq;
using Prometheus;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    public class PrometheusRunner : IStreamMiddleware
    {
        private PrometheusMetricServer MetricsServer { get; }

        public PrometheusRunner(int prometheusExporterEndpointPort)
        {
            MetricsServer = new PrometheusMetricServer(prometheusExporterEndpointPort);
        }

        #region IStreamMiddleware impl
        
        public void BeforeStart(IStreamConfig config)
        {
            MetricsServer.Start();
        }

        public void AfterStart(IStreamConfig config)
        {
            // nothing
        }

        public void BeforeStop(IStreamConfig config)
        {
            // nothing 
        }

        public void AfterStop(IStreamConfig config)
        {
            MetricsServer.Stop();
        }
        
        #endregion
        
        internal void Expose(IEnumerable<Sensor> sensors)
        {
            string MetricKey(StreamMetric metric) => $"{metric.Group}_{metric.Name}".Replace("-", "_");
            
            var registry = global::Prometheus.Metrics.NewCustomRegistry();
            var metricFactory = global::Prometheus.Metrics.WithCustomRegistry(registry);
            
            MetricsServer.ClearGauges(registry);

            foreach (var metric in sensors.SelectMany(s => s.Metrics))
            {
                var metricKey = MetricKey(metric.Value);
                
                var gauge = metricFactory.CreateGauge(metricKey, metric.Key.Description, 
                    new GaugeConfiguration {
                        LabelNames = metric.Key.Tags.Keys.ToArray()
                    });

                MetricsServer.AddGauge(gauge, metric);
            }
        }
    }
}