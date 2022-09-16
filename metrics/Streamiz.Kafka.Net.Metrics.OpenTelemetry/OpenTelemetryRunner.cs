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
        
        public void BeforeStart(IStreamConfig config)
        {
        }

        public void AfterStart(IStreamConfig config)
        {
        }

        public void BeforeStop(IStreamConfig config)
        {
        }

        public void AfterStop(IStreamConfig config)
        {
            meterProvider.ForceFlush();
            meterProvider.Dispose();
            openTelemetryMetricsExporter.Dispose();
        }
    }
}