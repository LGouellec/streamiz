using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class StreamTableJoinNode<K, V> : StreamGraphNode
    {
        private readonly ProcessorParameters<K, V> processorParameters;
        private readonly string[] storeNames;
        private readonly string otherJoinSideNodeName;

        public StreamTableJoinNode(
                string streamGraphNode,
                ProcessorParameters<K, V> processorParameters,
                string[] storeNames,
                string otherJoinSideNodeName) 
            : base(streamGraphNode)
        {
            this.processorParameters = processorParameters;
            this.storeNames = storeNames;
            this.otherJoinSideNodeName = otherJoinSideNodeName;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            // Stream - Table join (Global or KTable)
            builder.AddProcessor(processorParameters.ProcessorName, processorParameters.Processor, ParentNodeNames());

            // Steam - KTable join only
            if (!string.IsNullOrEmpty(otherJoinSideNodeName))
            {
                builder.ConnectProcessorAndStateStore(processorParameters.ProcessorName, storeNames);
            }
        }
    }
}
