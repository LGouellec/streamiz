using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    public interface ITopologyDescription
    {
        IEnumerable<ISubTopologyDescription> SubTopologies { get; }
    }

    public interface ISubTopologyDescription
    {
        string Id { get; }
        IEnumerable<INodeDescription> Nodes { get; }
    }

    public interface INodeDescription
    {
        string Name { get; }
        IEnumerable<INodeDescription> Next { get; }
        IEnumerable<INodeDescription> Previous { get; }
    }

    public interface ISourceNodeDescription : INodeDescription
    {
        IEnumerable<string> Topics { get; }
        Type TimestampExtractorType { get; }
    }

    public interface IProcessorNodeDescription : INodeDescription
    {
        IEnumerable<string> Stores { get; }
    }

    public interface ISinkNodeDescription : INodeDescription
    {
        string Topic { get; }
        Type TopicNameExtractorType { get; }
    }
}
