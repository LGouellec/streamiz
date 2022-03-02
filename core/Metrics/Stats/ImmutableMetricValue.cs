namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class ImmutableMetricValue<T> : IGauge<T>
    {
        private T value;

        public ImmutableMetricValue(T value)
        {
            this.value = value;
        }

        public T Value(MetricConfig config, long now)
            => value;
    }
}