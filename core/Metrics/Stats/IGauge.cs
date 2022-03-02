namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal interface IGauge<out T> : IMetricValueProvider
    {
        T Value(MetricConfig config, long now);
    }
}