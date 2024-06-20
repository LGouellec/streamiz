using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class AsyncNode<K, V, K1, V1> : StreamGraphNode
    {
        private class AsyncNodeRequest<TK, TV> : StreamGraphNode
        {
            private string SourceName { get; }
            private ISerDes<TK> KeySerdes { get; }
            private ISerDes<TV> ValueSerdes { get; }
            private string SinkName { get; }
            private string RepartitionTopic { get; }

            public AsyncNodeRequest(
                string streamGraphNode,
                string sourceName,
                ISerDes<TK> keySerdes,
                ISerDes<TV> valueSerdes,
                string sinkName,
                string repartitionTopic)
                : base(streamGraphNode)
            {
                SourceName = sourceName;
                KeySerdes = keySerdes;
                ValueSerdes = valueSerdes;
                SinkName = sinkName;
                RepartitionTopic = repartitionTopic;
            }

            public override void WriteToTopology(InternalTopologyBuilder builder)
            {
                builder.AddInternalTopic(RepartitionTopic, null);
                builder.AddSinkOperator(
                    new StaticTopicNameExtractor<TK, TV>(RepartitionTopic),
                    new DefaultRecordTimestampExtractor<TK, TV>(),
                    SinkName,
                    Produced<TK, TV>.Create(KeySerdes, ValueSerdes),
                    ParentNodeNames());
                builder.AddSourceOperator(
                    RepartitionTopic,
                    SourceName,
                    new ConsumedInternal<TK, TV>(SourceName, KeySerdes, ValueSerdes, new FailOnInvalidTimestamp()),
                    true);
            }
        }

        private class AsyncNodeResponse<TK, TV, TK1, TV1> : StreamGraphNode
        {
            public string SourceName { get; }
            public ProcessorParameters<TK, TV> ProcessorParameters { get; }
            public ISerDes<TK1> KeySerdes { get; }
            public ISerDes<TV1> ValueSerdes { get; }
            public string SinkName { get; }
            public string RepartitionTopic { get; }

            public AsyncNodeResponse(string streamGraphNode, string sourceName, ProcessorParameters<TK, TV> processorParameters, ISerDes<TK1> keySerdes, ISerDes<TV1> valueSerdes, string sinkName, string repartitionTopic)
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
                builder.AddSinkOperator(new StaticTopicNameExtractor<TK1, TV1>(RepartitionTopic),
                    new DefaultRecordTimestampExtractor<TK1, TV1>(),
                        SinkName,
                        Produced<TK1, TV1>.Create(KeySerdes, ValueSerdes),
                        ProcessorParameters.ProcessorName);
                builder.AddSourceOperator(
                        RepartitionTopic,
                        SourceName, 
                        new ConsumedInternal<TK1, TV1>(SourceName, KeySerdes, ValueSerdes, new FailOnInvalidTimestamp()));
            }

        }

        private class AsyncNodeRequestVoid<TK, TV> : StreamGraphNode
        {
            public string SourceName { get; }
            public ProcessorParameters<TK, TV> ProcessorParameters { get; }
            public ISerDes<TK> KeySerdes { get; }
            public ISerDes<TV> ValueSerdes { get; }
            public string RequestTopic { get; }
            public string SinkName { get; }

            public AsyncNodeRequestVoid(
                string streamGraphNode,
                string sourceName,
                string requestTopic,
                string sinkName,
                ProcessorParameters<TK, TV> processorParameters,
                ISerDes<TK> keySerdes,
                ISerDes<TV> valueSerdes)
                : base(streamGraphNode)
            {
                SourceName = sourceName;
                ProcessorParameters = processorParameters;
                KeySerdes = keySerdes;
                ValueSerdes = valueSerdes;
                RequestTopic = requestTopic;
                SinkName = sinkName;
            }
            
            public override void WriteToTopology(InternalTopologyBuilder builder)
            {
                builder.AddInternalTopic(RequestTopic, null);
                builder.AddSinkOperator(new StaticTopicNameExtractor<TK, TV>(RequestTopic),
                    new DefaultRecordTimestampExtractor<TK, TV>(),
                    SinkName,
                    Produced<TK, TV>.Create(KeySerdes, ValueSerdes),
                    ParentNodeNames());
                builder.AddSourceOperator(
                    RequestTopic,
                    SourceName, 
                    new ConsumedInternal<TK, TV>(SourceName, KeySerdes, ValueSerdes, new FailOnInvalidTimestamp()),
                    true);
                builder.AddProcessor(ProcessorParameters.ProcessorName, ProcessorParameters.Processor, SourceName);
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
            RequestNode = new AsyncNodeRequest<K, V>(
                requestSourceProcessorName,
                requestSourceProcessorName,
                requestSerDes.RequestKeySerDes,
                requestSerDes.RequestValueSerDes,
                requestSinkProcessorName,
                requestTopicName);

            ResponseNode = new AsyncNodeResponse<K,V,K1,V1>(
                responseSourceProcessorName,
                responseSourceProcessorName,
                processorParameters,
                responseSerDes.ResponseKeySerDes,
                responseSerDes.ResponseValueSerDes,
                responseSinkProcessorName,
                responseTopicName);
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

            RequestNode = new AsyncNodeRequestVoid<K,V>(
                requestSourceProcessorName,
                requestSourceProcessorName,
                requestTopicName,
                requestSinkProcessorName,
                processorParameters,
                requestSerDes.RequestKeySerDes,
                requestSerDes.RequestValueSerDes);
        }

        public StreamGraphNode RequestNode { get; }
        public StreamGraphNode ResponseNode { get; }

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