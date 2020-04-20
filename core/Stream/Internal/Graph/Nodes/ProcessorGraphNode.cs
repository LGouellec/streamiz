using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
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

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddProcessor(ProcessorParameters.ProcessorName, ProcessorParameters.Processor);
        }
    }
}
