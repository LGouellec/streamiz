using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class StateStoreNode : StreamGraphNode
    {
        protected readonly IStoreBuilder storeBuilder;
        protected readonly string[] processorNodeNames;

        public StateStoreNode(IStoreBuilder storeBuilder,
            string streamGraphNode,
            params string[] processorNodeNames) 
            : base(streamGraphNode)
        {
            this.storeBuilder = storeBuilder;
            this.processorNodeNames = processorNodeNames;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddStateStore(storeBuilder, false, processorNodeNames);
        }
    }
}
