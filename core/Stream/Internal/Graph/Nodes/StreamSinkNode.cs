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
        private ITopicNameExtractor<K, V> topicNameExtractor;
        private Produced<K, V> produced;

        public StreamSinkNode(ITopicNameExtractor<K, V> topicNameExtractor, string streamGraphNode, Produced<K, V> produced)
            : base(streamGraphNode)
        {
            this.topicNameExtractor = topicNameExtractor;
            this.produced = produced;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddSinkOperator(topicNameExtractor, this.streamGraphNode, produced);
        }
    }
}
