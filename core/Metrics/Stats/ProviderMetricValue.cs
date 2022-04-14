using System;

namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class ProviderMetricValue<T> : IGauge<T>
    {
        private readonly Func<T> provider;

        public ProviderMetricValue(Func<T> provider)
        {
            this.provider = provider;
        }

        public T Value(MetricConfig config, long now)
            => provider();

        object IGauge.Value(MetricConfig config, long now)
            => Value(config, now);
    }
}