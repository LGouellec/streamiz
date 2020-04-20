using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Stream
{
    public class Topology
    {
        internal InternalTopologyBuilder Builder { get; } = new InternalTopologyBuilder();

        internal Topology()
        {
        }
    }
}
