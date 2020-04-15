using Kafka.Streams.Net.Processors.Internal;

namespace Kafka.Streams.Net.Stream.Internal.Graph.Nodes
{
    internal class RootNode : StreamGraphNode
    {
        public RootNode() : base("ROOT-NODE")
        {
            HasWrittenToTopology = true;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
        }
    }
}
