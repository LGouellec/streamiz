using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics
{
    /// <summary>
    /// The <see cref="MetricName"/> class encapsulates a metric's name, logical group and its related tags.
    /// </summary>
    public class MetricName
    {
        /// <summary>
        /// The name of the metric
        /// </summary>
        public string Name { get; }
        
        /// <summary>
        /// Logical group name of the metrics to which this metric belongs.
        /// </summary>
        public string Group { get; }
        
        /// <summary>
        /// A human-readable description to include in the metric. This is optional.
        /// </summary>
        public string Description { get; }
        
        /// <summary>
        /// Additional key/value attributes of the metric. This is optional.
        /// </summary>
        public IDictionary<string, string> Tags { get; }

        /// <summary>
        /// Constructor of <see cref="MetricName"/> 
        /// </summary>
        /// <param name="metricName">The name of the metric</param>
        /// <param name="group">Logical group name of the metrics to which this metric belongs.</param>
        /// <param name="description">A human-readable description to include in the metric. It can be null.</param>
        /// <param name="tags">Additional key/value attributes of the metric. It can be null.</param>
        public MetricName(
            string metricName,
            string group,
            string description,
            IDictionary<string,string> tags)
        {
            Name = metricName;
            Group = @group;
            Description = description;
            Tags = tags ?? new Dictionary<string, string>();
        }

        /// <summary>
        /// Static method to create <see cref="MetricName"/> with only a name and a group
        /// </summary>
        /// <param name="name">The name of the metric</param>
        /// <param name="group">Logical group name of the metrics to which this metric belongs.</param>
        /// <returns>Return <see cref="MetricName"/> with description and tags empty </returns>
        public static MetricName NameAndGroup(string name, string group)
            => new MetricName(name, group, string.Empty, new Dictionary<string, string>());

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
            => obj is MetricName && ((MetricName) obj).Name.Equals(Name) &&
               ((MetricName) obj).Group.Equals(Group);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
            => Name.GetHashCode() + Group.GetHashCode();
    }
}