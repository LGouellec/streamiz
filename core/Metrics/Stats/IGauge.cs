namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal interface IGauge
    {
        object Value(MetricConfig config, long now);
    }
    
    internal interface IGauge<out T> : IMetricValueProvider, IGauge
    {
        T Value(MetricConfig config, long now);
    }
}