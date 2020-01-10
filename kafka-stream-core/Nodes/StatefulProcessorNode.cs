using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Nodes
{
    internal class StatefulProcessorNode<T, K, V> : ProcessorGraphNode<T, K, V>
        where T : NodeParameter<K, V>
    {
        public StatefulProcessorNode(string streamGraphNode, ProcessorParameters<T, K, V> @parameters) 
            : base(streamGraphNode, @parameters)
        {
        }
    }
}
