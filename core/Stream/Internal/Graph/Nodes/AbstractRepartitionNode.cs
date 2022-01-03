using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal abstract class AbstractRepartitionNode<K, V> : StreamGraphNode
    {
        public string SourceName { get; }
        public ProcessorParameters<K, V> ProcessorParameters { get; }
        public ISerDes<K> KeySerdes { get; }
        public ISerDes<V> ValueSerdes { get; }
        public string SinkName { get; }
        public string RepartitionTopic { get; }
        

        internal AbstractRepartitionNode(
            string streamGraphNode,
            string sourceName,
            ProcessorParameters<K, V> processorParameters,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes,
            string sinkName,
            string repartitionTopic) 
            : base(streamGraphNode)
        {
            SourceName = sourceName;
            ProcessorParameters = processorParameters;
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            SinkName = sinkName;
            RepartitionTopic = repartitionTopic;
        }
    }
}