using System.Collections.Generic;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics
{
    public class StreamMetric
    {
        internal StreamMetric(
            MetricName name,
            IMetricValueProvider metricValueProvider,
            MetricConfig config)
        {
            throw new System.NotImplementedException();
        }
        
        public object Value { get; }
        public string Name { get; private set; }
        public string Description { get; private set; }
        public string Group { get; private set; }
        public IReadOnlyDictionary<string, string> Tags { get; private set; } 
    }
}