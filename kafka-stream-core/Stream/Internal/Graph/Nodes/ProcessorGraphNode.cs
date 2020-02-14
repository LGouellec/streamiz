using kafka_stream_core.Nodes.Parameters;

namespace kafka_stream_core.Stream.Internal.Graph.Nodes
{
    internal abstract class ProcessorGraphNode : StreamGraphNode
    {
        internal ProcessorGraphNode(string streamGraphNode) :
        base(streamGraphNode)
        {
        }
    }

    internal class ProcessorGraphNode<K, V> : ProcessorGraphNode
    {
        public ProcessorParameters<K, V> ProcessorParameters { get; }

        public ProcessorGraphNode(string streamGraphNode, ProcessorParameters<K, V> @parameters) :
            base(streamGraphNode)
        {
            this.ProcessorParameters = @parameters;
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            builder.addProcessor(ProcessorParameters.ProcessorName, ProcessorParameters.Processor);
        }
    }
}
