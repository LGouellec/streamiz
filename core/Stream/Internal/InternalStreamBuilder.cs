using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;
using Streamiz.Kafka.Net.Table.Internal.Graph;
using Streamiz.Kafka.Net.Table.Internal.Graph.Nodes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class InternalStreamBuilder : INameProvider
    {
        private static string TABLE_SOURCE_SUFFIX = "-source";

        private int index = 0;
        private static readonly object _locker = new object();
        private readonly ILog logger = Logger.GetLogger(typeof(InternalStreamBuilder));

        internal IList<StreamGraphNode> nodes = new List<StreamGraphNode>();
        internal readonly RootNode root = new RootNode();

        internal readonly InternalTopologyBuilder internalTopologyBuilder;

        internal InternalStreamBuilder(InternalTopologyBuilder internalTopologyBuilder)
        {
            this.internalTopologyBuilder = internalTopologyBuilder;
        }

        #region Build Stream

        private int NextIndex { get { lock (_locker) return ++index; } }

        internal KStream<K, V> Stream<K, V>(string topic, ConsumedInternal<K, V> consumed)
        {
            var name = new Named(consumed.Named).OrElseGenerateWithPrefix(this, KStream.SOURCE_NAME);
            var node = new StreamSourceNode<K,V>(topic, name, consumed);
            this.AddGraphNode(root, node);
            KStream<K, V> stream = new KStream<K, V>(name, consumed.KeySerdes, consumed.ValueSerdes, new List<string> { name }, node, this);
            return stream;
        }

        public string NewProcessorName(string prefix)
        {
            return $"{prefix}-{NextIndex.ToString("D10")}";
        }
  
        internal void AddGraphNode(StreamGraphNode root, StreamGraphNode node)
        {
            logger.Debug($"Adding node {node} in root node {root}");
            root.AppendChild(node);
            nodes.Add(node);
        }

        #endregion

        #region Build Table

        public string NewStoreName(string prefix)
        {
            return $"{prefix}{KTable.STATE_STORE_NAME}{NextIndex.ToString("D10")}";
        }

        internal IKTable<K, V> Table<K, V>(string topic, ConsumedInternal<K, V> consumed, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            var sourceName = new Named(consumed.Named).SuffixWithOrElseGet(TABLE_SOURCE_SUFFIX, this, KStream.SOURCE_NAME);
            var tableSourceName = new Named(consumed.Named).OrElseGenerateWithPrefix(this, KTable.SOURCE_NAME);

            KTableSource<K, V> tableSource = new KTableSource<K,V>(materialized.StoreName , materialized.QueryableStoreName);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K,V>(tableSource, tableSourceName);

            var tableSourceNode = new TableSourceNode<K, V, IKeyValueStore<Bytes, byte[]>>(
                topic, tableSourceName, sourceName, consumed,
                materialized, processorParameters, false);

            this.AddGraphNode(root, tableSourceNode);

            return new KTable<K,V,V>(tableSourceName,
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

        internal void Build()
        {
            internalTopologyBuilder.BuildAndOptimizeTopology(root, nodes);
        }

        #endregion
    }
}