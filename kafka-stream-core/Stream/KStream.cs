using kafka_stream_core.SerDes;
using kafka_stream_core.Stream.Internal;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Stream
{
    public class KStream<K,V>
    {
        private readonly string nameNode;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;
        private readonly List<string> setSourceNodes;
        private readonly StreamGraphNode node;
        private readonly InternalStreamBuilder builder;

        internal KStream(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> setSourceNodes, StreamGraphNode node, InternalStreamBuilder builder)
        {
            nameNode = name;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
            this.setSourceNodes = setSourceNodes;
            this.node = node;
            this.builder = builder;
        }

        public KStream<K, V> filter(Func<K, V, bool> predicate)
        {
            string name = this.builder.newProcessorName("KSTREAM-FILTER-");
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(name, predicate), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            this.builder.addGraphNode(node, filterProcessorNode);
            return new KStream<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, filterProcessorNode, this.builder);
        }

        public KStream<K, V> filterNot(Func<K, V, bool> predicate)
        {
            string name = this.builder.newProcessorName("KSTREAM-FILTER-");
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(name, predicate, true), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            this.builder.addGraphNode(node, filterProcessorNode);
            return new KStream<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, filterProcessorNode, this.builder);
        }

        public void to(string topicName, Produced<K,V> produced)
        {
            string name = this.builder.newProcessorName("KSTREAM-SINK-");
            StreamSinkNode<K, V> sinkNode = new StreamSinkNode<K,V>(topicName, name, produced);
            this.builder.addGraphNode(node, sinkNode);
        }

        public void to(string topicName)
        {
            string name = this.builder.newProcessorName("KSTREAM-SINK-");
            StreamSinkNode<string, string> sinkNode = new StreamSinkNode<string, string>(topicName, name, Produced<string, string>.with(new StringSerDes(), new StringSerDes()));
            this.builder.addGraphNode(node, sinkNode);
        }
    }
}
