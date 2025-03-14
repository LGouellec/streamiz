using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    #region TopologyDescription

    internal class TopologyDescription : ITopologyDescription
    {
        private readonly IList<ISubTopologyDescription> subtopologies = new List<ISubTopologyDescription>();
        private readonly IList<IGlobalStoreDescription> globalStores = new List<IGlobalStoreDescription>();

        public IEnumerable<ISubTopologyDescription> SubTopologies => subtopologies;
        public IEnumerable<IGlobalStoreDescription> GlobalStores => globalStores;

        public TopologyDescription()
        {
        }

        public void AddSubtopology(ISubTopologyDescription sub)
        {
            if (!SubTopologies.Any(s => s.Id.Equals(sub.Id)))
                subtopologies.Add(sub);
        }

        internal void AddGlobalStore(GlobalStoreDescription globalStoreDescription)
        {
            if (!globalStores.Any(g => g.Id.Equals(globalStoreDescription.Id)))
                globalStores.Add(globalStoreDescription);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Topologies:");

            var subtopo = subtopologies.OrderBy(i => i.Id).ToArray();
            var global = globalStores.OrderBy(i => i.Id).ToArray();
            int expectedId = 0;
            int subtopoindex = subtopo.Length - 1;
            int globalindex = global.Length - 1;

            while (subtopoindex != -1 && globalindex != -1)
            {
                sb.Append("   ");
                var sub = subtopo[subtopoindex];
                var gl = global[globalindex];
                if (sub.Id.Equals(expectedId))
                {
                    sb.Append($"{sub}");
                    --subtopoindex;
                }
                else
                {
                    sb.Append($"{gl}");
                    --globalindex;
                }
                ++expectedId;
            }

            while (subtopoindex != -1)
            {
                sb.Append($"   {subtopo[subtopoindex]}");
                --subtopoindex;
            }

            while (globalindex != -1)
            {
                sb.Append($"   {global[globalindex]}");
                --globalindex;
            }

            return sb.ToString();
        }
    }

    #endregion

    #region SubTopologyDescription

    internal class SubTopologyDescription : ISubTopologyDescription
    {
        public int Id { get; }

        public IEnumerable<INodeDescription> Nodes { get; }

        public SubTopologyDescription(int id, IList<INodeDescription> nodes)
        {
            Id = id;
            Nodes = nodes;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Sub-topology: {Id}");
            foreach (var n in Nodes)
                sb.Append($"    {n}");
            return sb.ToString();
        }
    }

    #endregion

    #region GlobalStoreDescription


    internal class GlobalStoreDescription : IGlobalStoreDescription
    {
        public ISourceNodeDescription Source { get; }

        public IProcessorNodeDescription Processor { get; }

        public int Id { get; }

        public GlobalStoreDescription(string sourceName, string processorName, string storeName, string topicName, int id)
        {
            var source = new SourceNodeDescription(sourceName, topicName);
            var processor = new ProcessorNodeDescription(processorName, new List<string> { storeName });
            source.AddSuccessor(processor);
            processor.AddPredecessor(source);

            Id = id;
            Source = source;
            Processor = processor;
        }

        public override bool Equals(object obj)
        {
            return obj is GlobalStoreDescription description &&
                description.Source.Equals(Source) &&
                description.Processor.Equals(Processor);
        }
        public override int GetHashCode()
        {
            return Source.GetHashCode() + Processor.GetHashCode();
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Sub-topology: {Id} for global store (will not generate tasks)");
            sb.Append($"    {Source}").Append($"    {Processor}");
            return sb.ToString();
        }
    }

    #endregion

    #region NodeDescription

    internal abstract class NodeDescription : INodeDescription
    {
        private readonly IList<INodeDescription> next = new List<INodeDescription>();
        private readonly IList<INodeDescription> previous = new List<INodeDescription>();

        public string Name { get; }

        public IEnumerable<INodeDescription> Next => next;

        public IEnumerable<INodeDescription> Previous => previous;

        public NodeDescription(string name)
        {
            Name = name;
        }

        protected static string NodeNames(IEnumerable<INodeDescription> nodes)
            => nodes.Any() ? string.Join(", ", nodes.Select(n => n.Name)) : "none";

        public void AddPredecessor(INodeDescription node)
        {
            if (!previous.Contains(node))
                previous.Add(node);
        }

        public void AddSuccessor(INodeDescription node)
        {
            if (!next.Contains(node))
                next.Add(node);
        }

        public override bool Equals(object obj)
            => obj is INodeDescription description && description.Equals(this.Name);

        public override int GetHashCode()
            => Name.GetHashCode();
    }

    #endregion

    #region SourceNodeDescription

    internal class SourceNodeDescription : NodeDescription, ISourceNodeDescription
    {
        public IEnumerable<string> Topics { get; }

        public Type TimestampExtractorType { get; }

        public SourceNodeDescription(string name, string topic)
            : this(name, topic, null) { }

        public SourceNodeDescription(string name, string topic, Type timestampExtractorType)
            : base(name)
        {
            Topics = new List<string> { topic };
            TimestampExtractorType = timestampExtractorType;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Source: {Name} (topics: [{string.Join(",", Topics)}])");
            sb.AppendLine($"      --> {NodeNames(Next)}");
            return sb.ToString();
        }
    }

    #endregion

    #region ProcessorNodeDescription

    internal class ProcessorNodeDescription : NodeDescription, IProcessorNodeDescription
    {
        public IEnumerable<string> Stores { get; }

        public ProcessorNodeDescription(string name, IEnumerable<string> stores = null)
            : base(name)
        {
            Stores = stores;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Processor: {Name} (stores: [{string.Join(",", Stores)}])");
            sb.AppendLine($"      --> {NodeNames(Next)}");
            sb.AppendLine($"      <-- {NodeNames(Previous)}");
            return sb.ToString();
        }
    }

    #endregion

    #region SinkNodeDescription

    internal class SinkNodeDescription : NodeDescription, ISinkNodeDescription
    {
        public string Topic { get; }

        public Type TopicNameExtractorType { get; }

        public SinkNodeDescription(string name, string topic)
            : base(name)
        {
            Topic = topic;
            TopicNameExtractorType = null;
        }

        public SinkNodeDescription(string name, Type topicNameExtractorType)
            : base(name)
        {
            Topic = null;
            TopicNameExtractorType = topicNameExtractorType;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            if (!string.IsNullOrEmpty(Topic))
                sb.AppendLine($"Sink: {Name} (topic: {Topic})");
            else
                sb.AppendLine($"Sink: {Name} (extractor class: {TopicNameExtractorType.FullName})");

            sb.AppendLine($"      <-- {NodeNames(Previous)}");
            return sb.ToString();
        }
    }

    #endregion
}