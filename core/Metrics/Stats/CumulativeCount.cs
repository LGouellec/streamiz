namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class CumulativeCount : CumulativeSum
    {
        public override void Record(MetricConfig config, double value, long timeMs)
        {
            base.Record(config, 1, timeMs);
        }
    }
}