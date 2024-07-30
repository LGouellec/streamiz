using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal abstract class StreamSinkNode : StreamGraphNode
    {
        protected StreamSinkNode(string streamGraphNode)
            : base(streamGraphNode)
        {
        }
    }

    internal class StreamSinkNode<K, V> : StreamSinkNode
    {
        private readonly ITopicNameExtractor<K, V> topicNameExtractor;
        private readonly Produced<K, V> produced;
        private readonly IRecordTimestampExtractor<K, V> timestampExtractor;

        public StreamSinkNode(
            ITopicNameExtractor<K, V> topicNameExtractor,
            IRecordTimestampExtractor<K, V> timestampExtractor,
        string streamGraphNode, Produced<K, V> produced)
            : base(streamGraphNode)
        {
            this.topicNameExtractor = topicNameExtractor;
            this.timestampExtractor = timestampExtractor;
            this.produced = produced;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddSinkOperator(topicNameExtractor, timestampExtractor, streamGraphNode, produced, ParentNodeNames());
        }
    }
}
