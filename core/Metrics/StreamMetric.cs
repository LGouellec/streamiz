using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics
{
    /// <summary>
    /// A metric tracked for monitoring purposes.
    /// </summary>
    public class StreamMetric
    {
        private readonly MetricName metricName;
        private readonly IMetricValueProvider metricValueProvider;
        private readonly MetricConfig config;

        private object lastValue = -1L;

        internal StreamMetric(
            MetricName metricName,
            IMetricValueProvider metricValueProvider,
            MetricConfig config)
        {
            this.metricName = metricName;
            this.metricValueProvider = metricValueProvider;
            this.config = config;
        }

        /// <summary>
        /// The value of the metric, which may be measurable or a non-measurable gauge
        /// </summary>
        public object Value
        {
            get
            {
                Measure(DateTime.Now.GetMilliseconds());
                return lastValue;
            }
        }
        
        /// <summary>
        /// Name of the metric
        /// </summary>
        public string Name => this.metricName.Name;
        
        /// <summary>
        /// Description of the metric
        /// </summary>
        public string Description => this.metricName.Description;
        
        /// <summary>
        /// Logical group of the metric
        /// </summary>
        public string Group => metricName.Group;
        
        /// <summary>
        /// Additional key/value pairs
        /// </summary>
        public IReadOnlyDictionary<string, string> Tags => new ReadOnlyDictionary<string, string>(metricName.Tags);

        private void Measure(long now)
        {
            if (metricValueProvider is IMeasurable)
                lastValue = ((IMeasurable) metricValueProvider).Measure(config, now);
            else if (metricValueProvider is IGauge)
                lastValue = ((IGauge) metricValueProvider).Value(config, now);
        }

        internal void ChangeTagValue(string tag, string newValue)
            => metricName.Tags.AddOrUpdate(tag, newValue);
    }
}