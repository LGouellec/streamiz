using System.Linq.Expressions;
using Prometheus;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    public class PrometheusRunner : IStreamMiddleware
    {
        private readonly int prometheusExporterEndpointPort;
        private readonly MetricServer metricsServer;

        public PrometheusRunner(int prometheusExporterEndpointPort)
        {
            this.prometheusExporterEndpointPort = prometheusExporterEndpointPort;
            metricsServer = new MetricServer(prometheusExporterEndpointPort);
        }

        public void BeforeStart(IStreamConfig config)
        {
            metricsServer.Start();
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
            metricsServer.Stop();
        }
    }
}