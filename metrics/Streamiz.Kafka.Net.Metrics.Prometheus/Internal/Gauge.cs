using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    internal class Gauge
    {
        public string Key { get; set; }
        public string Description { get; set; }
        public IReadOnlyDictionary<string, string> Labels { get; set; }
        public double Value { get; private set; }

        public Gauge(string key, string description, IReadOnlyDictionary<string, string> labels)
        {
            Key = key;
            Description = description;
            Labels = labels;
        }

        public Gauge SetValue(double value)
        {
            Value = value;
            return this;
        }
    }
}