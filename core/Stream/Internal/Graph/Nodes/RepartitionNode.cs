using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class RepartitionNode<K, V> : AbstractRepartitionNode<K, V>
    {
        public RepartitionNode(string streamGraphNode, string sourceName, ProcessorParameters<K, V> processorParameters, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string sinkName, string repartitionTopic) 
            : base(streamGraphNode, sourceName, processorParameters, keySerdes, valueSerdes, sinkName, repartitionTopic)
        {
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            if (ProcessorParameters != null)
            {
                builder.AddInternalTopic(RepartitionTopic);
                builder.AddProcessor(ProcessorParameters.ProcessorName, ProcessorParameters.Processor,
                    ParentNodeNames());
                builder.AddSinkOperator(
                    new StaticTopicNameExtractor<K, V>(RepartitionTopic),
                    SinkName,
                    Produced<K, V>.Create(KeySerdes, ValueSerdes),
                    ProcessorParameters.ProcessorName);
                builder.AddSourceOperator(
                    RepartitionTopic,
                    SourceName,
                    new ConsumedInternal<K, V>(SourceName, KeySerdes, ValueSerdes, new FailOnInvalidTimestamp()));
            }
            else
            {
                builder.AddInternalTopic(RepartitionTopic);
                builder.AddSinkOperator(
                    new StaticTopicNameExtractor<K, V>(RepartitionTopic),
                    SinkName,
                    Produced<K, V>.Create(KeySerdes, ValueSerdes),
                    ParentNodeNames());
                builder.AddSourceOperator(
                    RepartitionTopic,
                    SourceName,
                    new ConsumedInternal<K, V>(SourceName, KeySerdes, ValueSerdes, new FailOnInvalidTimestamp()));
            }
        }
    }
}