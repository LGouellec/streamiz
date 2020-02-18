using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;

namespace kafka_stream_core.Stream.Internal.Graph.Nodes
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
        private TopicNameExtractor<K, V> topicNameExtractor;
        private Produced<K, V> produced;

        public StreamSinkNode(TopicNameExtractor<K, V> topicNameExtractor, string streamGraphNode, Produced<K, V> produced)
            : base(streamGraphNode)
        {
            this.topicNameExtractor = topicNameExtractor;
            this.produced = produced;
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            builder.addSinkOperator(topicNameExtractor, this.streamGraphNode, produced);
        }
    }
}
