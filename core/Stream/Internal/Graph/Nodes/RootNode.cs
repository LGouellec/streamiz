using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
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
