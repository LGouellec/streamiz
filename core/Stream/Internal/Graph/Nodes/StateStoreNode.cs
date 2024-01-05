using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class StateStoreNode : StreamGraphNode
    {
        private readonly IStoreBuilder storeBuilder;

        public StateStoreNode(IStoreBuilder storeBuilder, string streamGraphNode) : base(streamGraphNode)
        {
            this.storeBuilder = storeBuilder;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddStateStore(storeBuilder);
        }
    }
}
