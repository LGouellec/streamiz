using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// A meta representation of a <see cref="Topology"/>
    /// <p>
    /// The nodes of a topology are grouped into sub-topologies if they are connected.
    /// In contrast, two sub-topologies are not connected but can be linked to each other via topics, i.e., if one
    /// sub-topology writes into a topic and another sub-topology reads from the same topic.
    /// </p>
    /// When <see cref="KafkaStream.Start(System.Threading.CancellationToken)"/> is called, different sub-topologies will be constructed and executed as independent
    /// <see cref="StreamTask"/>.
    /// </summary>
    public interface ITopologyDescription
    {
        /// <summary>
        /// All sub-topologies of the represented topology.
        /// </summary>
        IEnumerable<ISubTopologyDescription> SubTopologies { get; }
    }

    /// <summary>
    /// A connected sub-graph of a <see cref="Topology"/>
    /// </summary>
    public interface ISubTopologyDescription
    {
        /// <summary>
        /// Internally assigned unique ID. 
        /// <para>ID is source topic name</para>
        /// </summary>
        int Id { get; }

        /// <summary>
        /// All nodes of this sub-topology.
        /// </summary>
        IEnumerable<INodeDescription> Nodes { get; }
    }

    /// <summary>
    /// A node of a topology. Can be a source, sink, or processor node.
    /// </summary>
    public interface INodeDescription
    {
        /// <summary>
        /// The name of the node. Will never be null.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The successor of this node within a sub-topology.
        /// Note, sinks do not have any successors.
        /// </summary>
        IEnumerable<INodeDescription> Next { get; }

        /// <summary>
        /// The predecessors of this node within a sub-topology.
        /// Note, sources do not have any predecessors.
        /// </summary>
        IEnumerable<INodeDescription> Previous { get; }
    }

    /// <summary>
    /// A source node of a topology.
    /// </summary>
    public interface ISourceNodeDescription : INodeDescription
    {
        /// <summary>
        /// The topic names this source node is reading from.
        /// <para>
        /// For moment, only one topic by source node
        /// </para>
        /// </summary>
        IEnumerable<string> Topics { get; }

        /// <summary>
        /// Type of timestampextractor
        /// </summary>
        Type TimestampExtractorType { get; }
    }

    /// <summary>
    /// A processor node of a topology.
    /// </summary>
    public interface IProcessorNodeDescription : INodeDescription
    {
        /// <summary>
        /// The names of all connected stores.
        /// </summary>
        IEnumerable<string> Stores { get; }
    }

    /// <summary>
    /// A sink node of a topology.
    /// </summary>
    public interface ISinkNodeDescription : INodeDescription
    {
        /// <summary>
        /// The topic name this sink node is writing to.
        /// Could be null if the topic name can only be dynamically determined based on <see cref="ITopicNameExtractor{K, V}"/>
        /// </summary>
        string Topic { get; }

        /// <summary>
        /// The <see cref="ITopicNameExtractor{K, V}"/> type class that this sink node uses to dynamically extract the topic name to write to.
        /// Could be null if the topic name is not dynamically determined.
        /// </summary>
        Type TopicNameExtractorType { get; }
    }
}