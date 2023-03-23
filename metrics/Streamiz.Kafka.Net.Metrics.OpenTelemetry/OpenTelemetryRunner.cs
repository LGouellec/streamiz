using System.Threading;
using OpenTelemetry.Metrics;

namespace Streamiz.Kafka.Net.Metrics.OpenTelemetry
{
    public class OpenTelemetryRunner : IStreamMiddleware
    {
        private readonly MeterProvider meterProvider;
        private readonly OpenTelemetryMetricsExporter openTelemetryMetricsExporter;

        public OpenTelemetryRunner(MeterProvider meterProvider,
            OpenTelemetryMetricsExporter openTelemetryMetricsExporter)
        {
            this.meterProvider = meterProvider;
            this.openTelemetryMetricsExporter = openTelemetryMetricsExporter;
        }
        
        public void BeforeStart(IStreamConfig config, CancellationToken token)
        {
        }

        public void AfterStart(IStreamConfig config, CancellationToken token)
        {
        }

        public void BeforeStop(IStreamConfig config, CancellationToken token)
        {
        }

        public void AfterStop(IStreamConfig config, CancellationToken token)
        {
            meterProvider.ForceFlush();
            meterProvider.Dispose();
            openTelemetryMetricsExporter.Dispose();
        }
    }
}