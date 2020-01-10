using kafka_stream_core.Nodes.Parameters;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Nodes
{
    internal class TransformKeyValueGraphNode<T, K, V, K1, V1> : ProcessorGraphNode<T, K, V>
        where T : NodeParameter<K, V>
    {
        public TransformKeyValueGraphNode(string streamGraphNode, ProcessorParameters<T, K, V> parameters) 
            : base(streamGraphNode, parameters)
        {
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            if (this.parameters.Param is KStreamTransform<K, V, K1, V1>)
            {
                KStreamTransform<K, V, K1, V1> transform = this.parameters.Param as KStreamTransform<K, V, K1, V1>;
                builder.setTransformOperator(this.streamGraphNode, transform.Transform);
            }
            else
                base.writeToTopology(builder);
        }
    }
}
