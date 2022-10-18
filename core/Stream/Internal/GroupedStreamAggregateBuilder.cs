using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class GroupedStreamAggregateBuilder<K, V>
    {
        private readonly InternalStreamBuilder builder;
        private readonly Grouped<K, V> grouped;
        private readonly List<string> sourceNodes;
        private readonly string name;
        private readonly bool repartitionRequired;
        private StreamGraphNode node;
        private StreamGraphNode repartitionNode;

        public GroupedStreamAggregateBuilder(InternalStreamBuilder builder, Grouped<K, V> grouped, List<string> sourceNodes, string name, bool repartitionRequired, StreamGraphNode node)
        {
            this.builder = builder;
            this.grouped = grouped;
            this.sourceNodes = sourceNodes;
            this.name = name;
            this.repartitionRequired = repartitionRequired;
            this.node = node;
        }

        internal IKTable<K, VR> Build<VR>(
            string functionName,
            StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder,
            IKStreamAggProcessorSupplier<K, K, V, VR> aggregateSupplier,
            string queryableStoreName,
            ISerDes<K> keySerdes,
            ISerDes<VR> valueSerdes)
        {
            var sourceName = Repartition(storeBuilder);
            
            StatefulProcessorNode<K, V> statefulProcessorNode =
               new StatefulProcessorNode<K, V>(
                   functionName,
                   new ProcessorParameters<K, V>(aggregateSupplier, functionName),
                   storeBuilder);

            builder.AddGraphNode(node, statefulProcessorNode);

            return new KTable<K, V, VR>(functionName,
                                    keySerdes,
                                    valueSerdes,
                                    sourceName.Equals(name) ? sourceNodes : sourceName.ToSingle().ToList(),
                                    queryableStoreName,
                                    aggregateSupplier,
                                    statefulProcessorNode,
                                    builder);
        }

        internal IKTable<KR, VR> BuildWindow<KR, VR>(
            string functionName,
            StoreBuilder<ITimestampedWindowStore<K, VR>> storeBuilder,
            IKStreamAggProcessorSupplier<K, KR, V, VR> aggregateSupplier,
            string queryableStoreName,
            ISerDes<KR> keySerdes,
            ISerDes<VR> valueSerdes)
        {
            var sourceName = Repartition(storeBuilder);
            
            StatefulProcessorNode<K, V> statefulProcessorNode =
               new StatefulProcessorNode<K, V>(
                   functionName,
                   new ProcessorParameters<K, V>(aggregateSupplier, functionName),
                   storeBuilder);

            builder.AddGraphNode(node, statefulProcessorNode);

            return new KTableGrouped<K, KR, V, VR>(functionName,
                                    keySerdes,
                                    valueSerdes,
                                    sourceName.Equals(name) ? sourceNodes : sourceName.ToSingle().ToList(),
                                    queryableStoreName,
                                    aggregateSupplier,
                                    statefulProcessorNode,
                                    builder);
        }

        private string Repartition(StoreBuilder storeBuilder)
        {
            if (repartitionRequired)
            {
                string suffix = grouped.Named ?? storeBuilder.Name;
                (string sourceName, RepartitionNode<K,V> repartNode) = KStream<K, V>.CreateRepartitionSource(suffix, grouped.Key, grouped.Value, builder);

                if (repartitionNode == null || grouped.Named == null)
                    repartitionNode = repartNode;
                
                builder.AddGraphNode(node, repartitionNode);
                node = repartitionNode;
                return sourceName;
            }

            return name;
        }
    }
}
