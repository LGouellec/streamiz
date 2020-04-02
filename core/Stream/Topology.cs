using kafka_stream_core.Processors.Internal;

namespace kafka_stream_core.Stream
{
    public class Topology
    {
        internal InternalTopologyBuilder Builder { get; } = new InternalTopologyBuilder();

        internal Topology()
        {
        }
    }
}
