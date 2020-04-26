using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    #region TopologyDescription

    internal class TopologyDescription : ITopologyDescription
    {
        private readonly IList<ISubTopologyDescription> subtopologies = new List<ISubTopologyDescription>();

        public IEnumerable<ISubTopologyDescription> SubTopologies => subtopologies;

        public TopologyDescription()
        {
        }

        public void AddSubtopology(ISubTopologyDescription sub)
        {
            if (!SubTopologies.Any(s => s.Id.Equals(sub.Id)))
                subtopologies.Add(sub);
        }
    }

    #endregion

    #region SubTopologyDescription

    internal class SubTopologyDescription : ISubTopologyDescription
    {
        public string Id { get; }

        public IEnumerable<INodeDescription> Nodes { get; }

        public SubTopologyDescription(string id, IList<INodeDescription> nodes)
        {
            Id = id;
            Nodes = nodes;
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

        public override bool Equals(object obj) => obj is INodeDescription && ((INodeDescription)obj).Equals(this.Name);

        public override int GetHashCode() => Name.GetHashCode();
    }

    #endregion

    #region SourceNodeDescription

    internal class SourceNodeDescription : NodeDescription, ISourceNodeDescription
    {
        public IEnumerable<string> Topics { get; }

        public Type TimestampExtractorType { get; }

        public SourceNodeDescription(string name, string topic) 
            : base(name)
        {
            Topics = new List<string> { topic };
            TimestampExtractorType = null;
        }

        public SourceNodeDescription(string name, Type timestampExtractorType)
            : base(name)
        {
            Topics = null;
            TimestampExtractorType = timestampExtractorType;
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
    }

    #endregion
}