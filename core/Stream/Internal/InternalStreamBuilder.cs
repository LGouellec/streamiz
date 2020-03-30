using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.State;
using kafka_stream_core.Stream.Internal.Graph;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using kafka_stream_core.Table;
using kafka_stream_core.Table.Internal;
using kafka_stream_core.Table.Internal.Graph;
using kafka_stream_core.Table.Internal.Graph.Nodes;
using log4net;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Stream.Internal
{
    internal class InternalStreamBuilder : NameProvider
    {
        private static string TABLE_SOURCE_SUFFIX = "-source";

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
            String name = newProcessorName(KStreamImpl<byte, byte>.SOURCE_NAME);
            var node = new StreamSourceNode<K,V>(topic, name, consumed);
            this.addGraphNode(root, node);
            KStreamImpl<K, V> stream = new KStreamImpl<K, V>(name, consumed.KeySerdes, consumed.ValueSerdes, new List<string> { name }, node, this);
            return stream;
        }

        public string newProcessorName(string prefix)
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

        #region Build Table

        public string newStoreName(string prefix)
        {
            return $"{prefix}{KTableImpl<byte, byte, byte>.STATE_STORE_NAME}{NextIndex.ToString("D10")}";
        }

        internal KTable<K, V> table<K, V>(string topic, Consumed<K, V> consumed, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
        {
            string sourceName = newProcessorName(KStreamImpl<byte, byte>.SOURCE_NAME);
            string tableSourceName = newProcessorName(KTableImpl<byte, byte, byte>.SOURCE_NAME);

            KTableSource<K, V> tableSource = new KTableSource<K,V>(materialized.StoreName, materialized.QueryableStoreName);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K,V>(tableSource, tableSourceName);

            var tableSourceNode = new TableSourceNode<K, V, KeyValueStore<Bytes, byte[]>>(
                topic, tableSourceName, sourceName, consumed,
                materialized, processorParameters, false);

            this.addGraphNode(root, tableSourceNode);

            return new KTableImpl<K,V,V>(tableSourceName,
                                    consumed.KeySerdes,
                                    consumed.ValueSerdes,
                                    new List<string> { sourceName },
                                    materialized.QueryableStoreName,
                                    tableSource,
                                    tableSourceNode,
                                    this);
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
