using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using log4net;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Stream.Internal
{
    internal class InternalStreamBuilder
    {
        private int index = 0;
        private readonly object _locker = new object();
        private readonly ILog logger = Logger.GetLogger(typeof(InternalStreamBuilder));

        internal IList<StreamGraphNode> nodes = new List<StreamGraphNode>();
        internal readonly RootNode root = new RootNode();

        private InternalTopologyBuilder internalTopologyBuilder;

        internal InternalStreamBuilder(InternalTopologyBuilder internalTopologyBuilder)
        {
            this.internalTopologyBuilder = internalTopologyBuilder;
        }

        #region Build Stream

        private int NextIndex { get { lock (_locker) return ++index; } }

        internal KStreamImpl<K, V> stream<K, V>(string topic, Consumed<K,V> consumed)
        {
            String name = newProcessorName("KSTREAM-SOURCE-");
            var node = new StreamSourceNode<K,V>(topic, name, consumed);
            this.addGraphNode(root, node);
            KStreamImpl<K, V> stream = new KStreamImpl<K, V>(name, consumed.KeySerdes, consumed.ValueSerdes, new List<string> { name }, node, this);
            return stream;
        }

        internal string newProcessorName(string prefix)
        {
            return $"{prefix}-{NextIndex.ToString("D10")}";
        }
    
        internal void addGraphNode(StreamGraphNode root, StreamGraphNode node)
        {
            if (logger.IsDebugEnabled)
                logger.Debug($"Adding node {node} in root node {root}");
            root.appendChild(node);
            nodes.Add(node);
        }

        #endregion

        #region Build Topology

        internal void build()
        {
            internalTopologyBuilder.buildAndOptimizeTopology(root, nodes);
        }

        #endregion
    }
}
