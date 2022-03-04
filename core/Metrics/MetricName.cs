using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics
{
    public class MetricName
    {
        public string Name { get; }
        public string Group { get; }
        public string Description { get; }
        public IDictionary<string, string> Tags { get; }

        public MetricName(
            string metricName,
            string group,
            string description,
            IDictionary<string,string> tags)
        {
            Name = metricName;
            Group = @group;
            Description = description;
            Tags = tags;
        }
    }
}