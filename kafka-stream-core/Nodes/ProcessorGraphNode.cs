using System;
using System.Collections.Generic;
using System.Text;

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
        private readonly ProcessorParameters<T, K, V> @parameters;

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
                builder.setFilterOperator(this.streamGraphNode, filter.Predicate);
            }
            // TODO :
        }
    }
}
