using kafka_stream_core.Nodes;
using kafka_stream_core.Nodes.Parameters;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using System;
using System.Collections.Generic;

namespace kafka_stream_core
{
    internal class InternalStreamBuilder
    {
        private int index = 0;
        private readonly object _locker = new object();
        private IList<StreamGraphNode> sourcesNodes = new List<StreamGraphNode>();
        private readonly RootNode root = new RootNode();

        private InternalTopologyBuilder internalTopologyBuilder;

        internal InternalStreamBuilder(InternalTopologyBuilder internalTopologyBuilder)
        {
            this.internalTopologyBuilder = internalTopologyBuilder;
        }

        #region Build Stream

        private int NextIndex { get { lock (_locker) return ++index; } }

        internal KStream<K, V> stream<K, V>(string topic, Consumed<K,V> consumed)
        {
            String name = newProcessorName("KSTREAM-SOURCE-");
            var node = new StreamSourceNode<K,V>(topic, name, consumed);
            this.addGraphNode(root, node);
            KStream<K, V> stream = new KStream<K, V>(name, consumed.KeySerdes, consumed.ValueSerdes, new List<string> { name }, node, this);
            return stream;
        }

        internal string newProcessorName(string prefix)
        {
            return $"{prefix}-{NextIndex.ToString("D10")}";
        }
    
        internal void addGraphNode(StreamGraphNode root, StreamGraphNode node)
        {
            root.appendChild(node);
            sourcesNodes.Add(node);
        }

        #endregion

        #region Build Topology

        internal void build()
        {
            internalTopologyBuilder.buildAndOptimizeTopology(root, sourcesNodes);
        }

        #endregion
    }
}
