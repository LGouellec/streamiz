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

        public static MetricName NameAndGroup(string name, string group)
            => new MetricName(name, group, string.Empty, new Dictionary<string, string>());

        public override bool Equals(object obj)
            => obj is MetricName && ((MetricName) obj).Name.Equals(Name) &&
               ((MetricName) obj).Group.Equals(Group);

        public override int GetHashCode()
            => Name.GetHashCode() + Group.GetHashCode();
    }
}