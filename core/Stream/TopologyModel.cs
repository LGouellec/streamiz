using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Kind of node in a topology.
    /// </summary>
    public static class TopologyNodeType
    {
        /// <summary>
        /// Source node (reads from one or more Kafka topics)
        /// </summary>
        public const string Source = "source";

        /// <summary>
        /// Processor node
        /// </summary>
        public const string Processor = "processor";

        /// <summary>
        /// Sink node (writes to a Kafka topic)
        /// </summary>
        public const string Sink = "sink";
    }

    /// <summary>
    /// Topics and state stores used by a <see cref="TopologyModel"/> or a <see cref="SubTopologyModel"/>.
    /// </summary>
    public abstract class TopologyResourcesModel
    {
        /// <summary>
        /// Topics read (for a topology, including the global stores' topics, and excluding the repartition topics
        /// listed in <see cref="RepartitionTopics"/>).
        /// </summary>
        [JsonProperty("sourceTopics", Order = 100)]
        public List<string> SourceTopics { get; set; } = new();

        /// <summary>
        /// Topics statically written (for a topology, excluding the repartition topics listed in
        /// <see cref="RepartitionTopics"/>).
        /// </summary>
        [JsonProperty("sinkTopics", Order = 101)]
        public List<string> SinkTopics { get; set; } = new();

        /// <summary>
        /// State stores used (for a topology, including the global stores).
        /// </summary>
        [JsonProperty("stateStores", Order = 102)]
        public List<string> StateStores { get; set; } = new();

        /// <summary>
        /// Internal repartition topics (only resolved when described from a <see cref="KafkaStream"/>).
        /// </summary>
        [JsonProperty("repartitionTopics", Order = 103)]
        public List<string> RepartitionTopics { get; set; } = new();

        /// <summary>
        /// Internal changelog topics (only resolved when described from a <see cref="KafkaStream"/>).
        /// </summary>
        [JsonProperty("changelogTopics", Order = 104)]
        public List<string> ChangelogTopics { get; set; } = new();
    }

    /// <summary>
    /// Serializable, machine-readable representation of a <see cref="Topology"/>.
    /// Build it from an <see cref="ITopologyDescription"/> with <see cref="TopologyDescriptionExtensions.ToModel"/>
    /// or from a running application with <see cref="KafkaStream.DescribeTopology"/> (which also resolves internal topics
    /// like repartition and changelog topics).
    /// Use <see cref="ToJson"/> to get its JSON representation, and <see cref="ToString"/> to get the classic text
    /// representation (same output as <see cref="Topology.Describe"/>).
    /// </summary>
    public sealed class TopologyModel : TopologyResourcesModel
    {
        [JsonIgnore]
        internal ITopologyDescription Description { get; set; }

        /// <summary>
        /// All sub-topologies.
        /// </summary>
        [JsonProperty("subTopologies")]
        public List<SubTopologyModel> SubTopologies { get; set; } = new();

        /// <summary>
        /// All global stores.
        /// </summary>
        [JsonProperty("globalStores")]
        public List<GlobalStoreModel> GlobalStores { get; set; } = new();

        /// <summary>
        /// Serialize this model in JSON.
        /// </summary>
        /// <param name="indented">true to indent the JSON output</param>
        /// <returns>JSON representation of the topology</returns>
        public string ToJson(bool indented = true)
            => JsonConvert.SerializeObject(this, indented ? Formatting.Indented : Formatting.None, JsonSettings);

        /// <summary>
        /// Text representation of the topology (same output as <see cref="Topology.Describe"/>).
        /// </summary>
        /// <returns>text representation of the topology</returns>
        public override string ToString()
            => Description?.ToString() ?? base.ToString();

        internal static readonly JsonSerializerSettings JsonSettings = new()
        {
            NullValueHandling = NullValueHandling.Ignore
        };

        internal void ComputeAggregates()
        {
            var nodes = SubTopologies.SelectMany(s => s.Nodes).ToList();
            var repartitionTopics = new HashSet<string>(SubTopologies.SelectMany(s => s.RepartitionTopics));
            // repartition topics are both read and written by the topology, they are listed in RepartitionTopics only
            SourceTopics = nodes.Where(n => n.Topics != null).SelectMany(n => n.Topics)
                .Concat(GlobalStores.Select(g => g.Topic))
                .Where(t => t != null && !repartitionTopics.Contains(t)).Distinct().OrderBy(t => t).ToList();
            SinkTopics = nodes.Where(n => n.Topic != null).Select(n => n.Topic)
                .Where(t => !repartitionTopics.Contains(t)).Distinct().OrderBy(t => t).ToList();
            StateStores = nodes.Where(n => n.Stores != null).SelectMany(n => n.Stores)
                .Concat(GlobalStores.Select(g => g.Store))
                .Where(t => t != null).Distinct().OrderBy(t => t).ToList();
            RepartitionTopics = SubTopologies.SelectMany(s => s.RepartitionTopics)
                .Distinct().OrderBy(t => t).ToList();
            ChangelogTopics = SubTopologies.SelectMany(s => s.ChangelogTopics)
                .Distinct().OrderBy(t => t).ToList();
        }
    }

    /// <summary>
    /// Serializable representation of a sub-topology.
    /// </summary>
    public sealed class SubTopologyModel : TopologyResourcesModel
    {
        /// <summary>
        /// Sub-topology id
        /// </summary>
        [JsonProperty("id")]
        public int Id { get; set; }

        /// <summary>
        /// Nodes of this sub-topology
        /// </summary>
        [JsonProperty("nodes")]
        public List<TopologyNodeModel> Nodes { get; set; } = new();
    }

    /// <summary>
    /// Serializable representation of a global store.
    /// </summary>
    public sealed class GlobalStoreModel
    {
        /// <summary>
        /// Global store sub-topology id
        /// </summary>
        [JsonProperty("id")]
        public int Id { get; set; }

        /// <summary>
        /// Global topic (also used as changelog topic)
        /// </summary>
        [JsonProperty("topic")]
        public string Topic { get; set; }

        /// <summary>
        /// State store name
        /// </summary>
        [JsonProperty("store")]
        public string Store { get; set; }

        /// <summary>
        /// Source node reading the global topic
        /// </summary>
        [JsonProperty("source")]
        public TopologyNodeModel Source { get; set; }

        /// <summary>
        /// Processor node maintaining the global store
        /// </summary>
        [JsonProperty("processor")]
        public TopologyNodeModel Processor { get; set; }
    }

    /// <summary>
    /// Serializable representation of a topology node (source, processor or sink).
    /// </summary>
    public sealed class TopologyNodeModel
    {
        /// <summary>
        /// Node name
        /// </summary>
        [JsonProperty("name")]
        public string Name { get; set; }

        /// <summary>
        /// Node type, see <see cref="TopologyNodeType"/>
        /// </summary>
        [JsonProperty("type")]
        public string Type { get; set; }

        /// <summary>
        /// Topics read (source node only)
        /// </summary>
        [JsonProperty("topics")]
        public List<string> Topics { get; set; }

        /// <summary>
        /// Timestamp extractor type (source node only, if any)
        /// </summary>
        [JsonProperty("timestampExtractor")]
        public string TimestampExtractor { get; set; }

        /// <summary>
        /// Topic written (sink node only, null if dynamically resolved)
        /// </summary>
        [JsonProperty("topic")]
        public string Topic { get; set; }

        /// <summary>
        /// Topic name extractor type (sink node only, when the topic is dynamically resolved)
        /// </summary>
        [JsonProperty("topicNameExtractor")]
        public string TopicNameExtractor { get; set; }

        /// <summary>
        /// Connected state stores (processor node only)
        /// </summary>
        [JsonProperty("stores")]
        public List<string> Stores { get; set; }

        /// <summary>
        /// Name of the successors nodes
        /// </summary>
        [JsonProperty("successors")]
        public List<string> Successors { get; set; } = new();

        /// <summary>
        /// Name of the predecessors nodes
        /// </summary>
        [JsonProperty("predecessors")]
        public List<string> Predecessors { get; set; } = new();
    }

    /// <summary>
    /// Extensions to convert a <see cref="ITopologyDescription"/> into a machine-readable model.
    /// </summary>
    public static class TopologyDescriptionExtensions
    {
        /// <summary>
        /// Convert a topology description into a serializable <see cref="TopologyModel"/>.
        /// </summary>
        /// <param name="description">topology description</param>
        /// <returns>the serializable model</returns>
        public static TopologyModel ToModel(this ITopologyDescription description)
        {
            var model = new TopologyModel { Description = description };

            foreach (var sub in description.SubTopologies.OrderBy(s => s.Id))
            {
                var nodes = sub.Nodes.Select(ToNodeModel).OrderBy(n => NodeOrder(n.Type)).ThenBy(n => n.Name).ToList();
                model.SubTopologies.Add(new SubTopologyModel
                {
                    Id = sub.Id,
                    Nodes = nodes,
                    SourceTopics = nodes.Where(n => n.Topics != null).SelectMany(n => n.Topics).Distinct().ToList(),
                    SinkTopics = nodes.Where(n => n.Topic != null).Select(n => n.Topic).Distinct().ToList(),
                    StateStores = nodes.Where(n => n.Stores != null).SelectMany(n => n.Stores).Distinct().ToList()
                });
            }

            foreach (var global in description.GlobalStores.OrderBy(g => g.Id))
            {
                model.GlobalStores.Add(new GlobalStoreModel
                {
                    Id = global.Id,
                    Topic = global.Source?.Topics?.FirstOrDefault(),
                    Store = global.Processor?.Stores?.FirstOrDefault(),
                    Source = global.Source != null ? ToNodeModel(global.Source) : null,
                    Processor = global.Processor != null ? ToNodeModel(global.Processor) : null
                });
            }

            model.ComputeAggregates();
            return model;
        }

        /// <summary>
        /// Serialize a topology description in JSON.
        /// </summary>
        /// <param name="description">topology description</param>
        /// <param name="indented">true to indent the JSON output</param>
        /// <returns>JSON representation of the topology</returns>
        public static string ToJson(this ITopologyDescription description, bool indented = true)
            => description.ToModel().ToJson(indented);

        private static int NodeOrder(string type) => type switch
        {
            TopologyNodeType.Source => 0,
            TopologyNodeType.Processor => 1,
            _ => 2
        };

        private static TopologyNodeModel ToNodeModel(INodeDescription node)
        {
            var model = new TopologyNodeModel
            {
                Name = node.Name,
                Successors = node.Next?.Select(n => n.Name).ToList() ?? new List<string>(),
                Predecessors = node.Previous?.Select(n => n.Name).ToList() ?? new List<string>()
            };

            switch (node)
            {
                case ISourceNodeDescription source:
                    model.Type = TopologyNodeType.Source;
                    model.Topics = source.Topics?.ToList() ?? new List<string>();
                    model.TimestampExtractor = source.TimestampExtractorType?.FullName;
                    break;
                case ISinkNodeDescription sink:
                    model.Type = TopologyNodeType.Sink;
                    model.Topic = sink.Topic;
                    model.TopicNameExtractor = sink.TopicNameExtractorType?.FullName;
                    break;
                case IProcessorNodeDescription processor:
                    model.Type = TopologyNodeType.Processor;
                    model.Stores = processor.Stores?.ToList() ?? new List<string>();
                    break;
            }

            return model;
        }
    }
}
