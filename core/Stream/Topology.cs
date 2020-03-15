using kafka_stream_core.Processors.Internal;

namespace kafka_stream_core.Stream
{
    public class Topology
    {
        public enum AutoOffsetReset
        {
            EARLIEST, LATEST
        }

        internal InternalTopologyBuilder Builder { get; } = new InternalTopologyBuilder();

        internal Topology()
        {
        }
    }
}
