using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Metrics
{
    internal class MetricUtils
    {
        public static void ExportMetrics(
            StreamMetricsRegistry registry,
            IStreamConfig config,
            string threadName)
        {
            var sensors = registry.GetThreadScopeSensor(threadName);
            Task.Factory.StartNew(() => config.MetricsReporter?.Invoke(sensors));
        }
    }
}