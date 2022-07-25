namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal interface IMeasurable : IMetricValueProvider
    {
        double Measure(MetricConfig config, long now);
    }
}