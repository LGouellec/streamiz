using kafka_stream_core.Nodes.Parameters;

namespace kafka_stream_core.Nodes
{
    internal abstract class ProcessorGraphNode : StreamGraphNode
    {
        internal ProcessorGraphNode(string streamGraphNode) :
        base(streamGraphNode)
        {
        }
    }

    internal class ProcessorGraphNode<T, K, V> : ProcessorGraphNode
        where T : NodeParameter<K, V>
    {
        protected readonly ProcessorParameters<T, K, V> @parameters;

        public ProcessorGraphNode(string streamGraphNode, ProcessorParameters<T, K, V> @parameters) :
            base(streamGraphNode)
        {
            this.@parameters = @parameters;
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            if(this.parameters.Param is KStreamFilter<K, V>)
            {
                KStreamFilter<K, V> filter = this.parameters.Param as KStreamFilter<K, V>;
                builder.setFilterOperator(this.streamGraphNode, filter.Predicate, filter.Not);
            }
            // TODO :
        }
    }
}
