using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class AsyncNode<K, V, K1, V1> : StreamGraphNode
    {
        private readonly string requestSinkProcessorName;
        private readonly string requestSourceProcessorName;
        private readonly string requestTopicName;
        private readonly string responseSinkProcessorName;
        private readonly string responseSourceProcessorName;
        private readonly string responseTopicName;
        private readonly RequestSerDes<K, V> requestSerDes;
        private readonly ResponseSerDes<K1, V1> responseSerDes;
        private readonly ProcessorParameters<K, V> processorParameters;

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
            this.requestSinkProcessorName = requestSinkProcessorName;
            this.requestSourceProcessorName = requestSourceProcessorName;
            this.requestTopicName = requestTopicName;
            this.responseSinkProcessorName = responseSinkProcessorName;
            this.responseSourceProcessorName = responseSourceProcessorName;
            this.responseTopicName = responseTopicName;
            this.requestSerDes = requestSerDes;
            this.responseSerDes = responseSerDes;
            this.processorParameters = processorParameters;

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
        }

        public RepartitionNode<K, V> RequestNode { get; }
        public AsyncNodeResponse<K, V, K1, V1> ResponseNode { get; }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            /*
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