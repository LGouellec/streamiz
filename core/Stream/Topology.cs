using Kafka.Streams.Net.Processors.Internal;

namespace Kafka.Streams.Net.Stream
{
    public class Topology
    {
        internal InternalTopologyBuilder Builder { get; } = new InternalTopologyBuilder();

        internal Topology()
        {
        }
    }
}
