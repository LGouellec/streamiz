using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class AsyncNode<K, V, K1, V1> : StreamGraphNode
    {
        internal class AsyncNodeResponse<K, V, K1, V1> : StreamGraphNode
        {
            public string SourceName { get; }
            public ProcessorParameters<K, V> ProcessorParameters { get; }
            public ISerDes<K1> KeySerdes { get; }
            public ISerDes<V1> ValueSerdes { get; }
            public string SinkName { get; }
            public string RepartitionTopic { get; }

            public AsyncNodeResponse(string streamGraphNode, string sourceName, ProcessorParameters<K, V> processorParameters, ISerDes<K1> keySerdes, ISerDes<V1> valueSerdes, string sinkName, string repartitionTopic)
                : base(streamGraphNode)
            {
                SourceName = sourceName;
                ProcessorParameters = processorParameters;
                KeySerdes = keySerdes;
                ValueSerdes = valueSerdes;
                SinkName = sinkName;
                RepartitionTopic = repartitionTopic;
            }
            
            public override void WriteToTopology(InternalTopologyBuilder builder)
            {
                builder.AddInternalTopic(RepartitionTopic, null);
                builder.AddProcessor(ProcessorParameters.ProcessorName, ProcessorParameters.Processor, ParentNodeNames());
                builder.AddSinkOperator(new StaticTopicNameExtractor<K1, V1>(RepartitionTopic),
                        SinkName,
                        Produced<K1, V1>.Create(KeySerdes, ValueSerdes),
                        ProcessorParameters.ProcessorName);
                builder.AddSourceOperator(
                        RepartitionTopic,
                        SourceName, 
                        new ConsumedInternal<K1, V1>(SourceName, KeySerdes, ValueSerdes, new FailOnInvalidTimestamp()));
            }

        }

        public AsyncNode(
            string asyncProcessorName,
            string requestSinkProcessorName,
            string requestSourceProcessorName,
            string requestTopicName,
            string responseSinkProcessorName,
            string responseSourceProcessorName,
            string responseTopicName,
            RequestSerDes<K, V> requestSerDes,
            ResponseSerDes<K1, V1> responseSerDes,
            ProcessorParameters<K, V> processorParameters) 
            : base(asyncProcessorName)
        {
            RequestNode = new RepartitionNode<K, V>(
                requestSourceProcessorName,
                requestSourceProcessorName,
                null,
                requestSerDes.RequestKeySerDes,
                requestSerDes.RequestValueSerDes,
                requestSinkProcessorName,
                requestTopicName)
            {
                StreamPartitioner = null,
                NumberOfPartition = null
            };

            ResponseNode = new AsyncNodeResponse<K,V,K1,V1>(
                responseSourceProcessorName,
                responseSourceProcessorName,
                processorParameters,
                responseSerDes.ResponseKeySerDes,
                responseSerDes.ResponseValueSerDes,
                responseSinkProcessorName,
                responseTopicName);

            ContinueStreaming = true;
        }
        
        public AsyncNode(
            string asyncProcessorName,
            string requestSinkProcessorName,
            string requestSourceProcessorName,
            string requestTopicName,
            RequestSerDes<K, V> requestSerDes,
            ProcessorParameters<K, V> processorParameters) 
            : base(asyncProcessorName)
        {
            ResponseNode = null;

            RequestNode = new AsyncNodeResponse<K,V,K,V>(
                requestSourceProcessorName,
                requestSourceProcessorName,
                processorParameters,
                requestSerDes.RequestKeySerDes,
                requestSerDes.RequestValueSerDes,
                requestSinkProcessorName,
                requestTopicName);

            ContinueStreaming = false;
        }

        public StreamGraphNode RequestNode { get; }
        public StreamGraphNode ResponseNode { get; }
        public bool ContinueStreaming { get; }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            /* KEEP IN MY MIND
            builder.AddSinkOperator(
                new StaticTopicNameExtractor<K, V>(requestTopicName),
                requestSinkProcessorName,
                Produced<K, V>.Create(requestSerDes.RequestKeySerDes, requestSerDes.RequestValueSerDes),
                ParentNodeNames());

            builder.AddInternalTopic(requestTopicName, null);
            builder.AddInternalTopic(responseTopicName, null);
            
            builder.AddSourceOperator(
                requestTopicName,
                requestSourceProcessorName,
                new ConsumedInternal<K, V>(requestSourceProcessorName, requestSerDes.RequestKeySerDes, requestSerDes.RequestValueSerDes, new FailOnInvalidTimestamp()),
                true);
            
            // scd repartition node
            builder.AddProcessor(
                processorParameters.ProcessorName,
                processorParameters.Processor,
                requestSourceProcessorName);
            
            builder.AddSinkOperator(
                new StaticTopicNameExtractor<K1, V1>(responseTopicName),
                responseSinkProcessorName,
                Produced<K1, V1>.Create(responseSerDes.ResponseKeySerDes, responseSerDes.ResponseValueSerDes),
                streamGraphNode);

            builder.AddSourceOperator(
                responseTopicName,
                responseSourceProcessorName,
                new ConsumedInternal<K1, V1>(responseSourceProcessorName, responseSerDes.ResponseKeySerDes, responseSerDes.ResponseValueSerDes, new FailOnInvalidTimestamp()));
            */
        }
    }
}