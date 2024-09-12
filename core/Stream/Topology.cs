using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// A logical representation of a <see cref="ProcessorTopology"/>
    /// A topology is an acyclic graph of sources, processors, and sinks.
    /// A source is a node in the graph that consumes one or more Kafka topics and forwards them to its
    /// successor nodes.
    /// A processor is a node in the graph that receives input records from upstream nodes, processes the
    /// records, and optionally forwarding new records to one or all of its downstream nodes.
    /// Finally, a sink is a node in the graph that receives records from upstream nodes and writes them to
    /// a Kafka topic.
    /// A <see cref="Topology"/> allows you to construct an acyclic graph of these nodes, and then passed into a new
    /// <see cref="KafkaStream"/> instance that will then <see cref="KafkaStream.StartAsync(System.Threading.CancellationToken?)"/> begin consuming, processing, and producing
    /// records}.
    /// </summary>
    public class Topology
    {
        internal InternalTopologyBuilder Builder { get; } = new();

        internal Topology()
        {
        }

        /// <summary>
        /// Returns a description of the specified <see cref="Topology"/>.
        /// Can using to create a schema about your topolgy.
        /// </summary>
        /// <returns>a description of the topology.</returns>
        public ITopologyDescription Describe() => Builder.Describe();
    }
}
