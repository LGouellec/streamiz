namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal interface IStat
    {
        void Record(MetricConfig config, double value, long timeMs);
    }
}