using kafka_stream_core.Processors.Internal;

namespace kafka_stream_core.Stream.Internal.Graph.Nodes
{
    internal class StreamSourceNode<K, V> : StreamGraphNode
    {
        private string topicName;
        private Consumed<K, V> consumed;

        public StreamSourceNode(string topicName, string streamGraphNode, Consumed<K,V> consumed) 
            : base(streamGraphNode)
        {
            this.topicName = topicName;
            this.consumed = consumed;
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            builder.addSourceOperator(topicName, this.streamGraphNode, consumed);
        }
    }
}
