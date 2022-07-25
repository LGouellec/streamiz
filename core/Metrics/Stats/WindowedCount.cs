namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class WindowedCount : WindowedSum
    {
        protected override void Update(Sample sample, MetricConfig config, double value, long timeMs)
        {
            base.Update(sample, config, 1.0, timeMs);
        }
    }
}