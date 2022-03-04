using System.Collections.Generic;
using System.Collections.ObjectModel;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics
{
    public class StreamMetric
    {
        private readonly MetricName metricName;
        private readonly IMetricValueProvider metricValueProvider;
        private readonly MetricConfig config;

        private object lastValue;

        internal StreamMetric(
            MetricName metricName,
            IMetricValueProvider metricValueProvider,
            MetricConfig config)
        {
            this.metricName = metricName;
            this.metricValueProvider = metricValueProvider;
            this.config = config;
        }

        public object Value => lastValue;
        public string Name => this.metricName.Name;
        public string Description => this.metricName.Description;
        public string Group => metricName.Group;
        public IReadOnlyDictionary<string, string> Tags => new ReadOnlyDictionary<string, string>(metricName.Tags);

        internal void Measure(long now)
        {
            if (metricValueProvider is IMeasurable)
                lastValue = ((IMeasurable) metricValueProvider).Measure(config, now);
            else if (metricValueProvider is IGauge)
                lastValue = ((IGauge) metricValueProvider).Value(config, now);
        }
    }
}