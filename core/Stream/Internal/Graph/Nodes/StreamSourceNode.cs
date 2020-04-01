using kafka_stream_core.Processors.Internal;

namespace kafka_stream_core.Stream.Internal.Graph.Nodes
{
    internal class StreamSourceNode<K, V> : StreamGraphNode
    {
        protected string topicName;
        protected Consumed<K, V> consumed;

        public StreamSourceNode(string topicName, string streamGraphNode, Consumed<K, V> consumed) 
            : base(streamGraphNode)
        {
            this.topicName = topicName;
            this.consumed = consumed;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddSourceOperator(topicName, this.streamGraphNode, consumed);
        }
    }
}
