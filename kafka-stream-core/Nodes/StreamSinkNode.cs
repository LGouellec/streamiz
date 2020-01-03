using kafka_stream_core.Nodes.Parameters;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Nodes
{
    internal abstract class StreamSinkNode : StreamGraphNode
    {
        internal StreamSinkNode(string streamGraphNode)
            : base(streamGraphNode)
        {
        }
    }

    internal class StreamSinkNode<K, V> : StreamSinkNode
    {
        private string topicName;
        private Produced<K, V> produced;

        public StreamSinkNode(string topicName, string streamGraphNode, Produced<K, V> produced)
            : base(streamGraphNode)
        {
            this.topicName = topicName;
            this.produced = produced;
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            builder.setSinkOperator(topicName, this.streamGraphNode, produced);
        }
    }
}
