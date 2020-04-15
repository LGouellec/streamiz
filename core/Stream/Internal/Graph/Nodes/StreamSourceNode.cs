using Kafka.Streams.Net.Processors.Internal;

namespace Kafka.Streams.Net.Stream.Internal.Graph.Nodes
{
    internal class StreamSourceNode<K, V> : StreamGraphNode
    {
        protected string topicName;
        protected ConsumedInternal<K, V> consumed;

        public StreamSourceNode(string topicName, string streamGraphNode, ConsumedInternal<K, V> consumed) 
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
