using System.Collections.Generic;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class GroupedStreamAggregateBuilder<K, V>
    {
        private InternalStreamBuilder builder;
        private Grouped<K, V> grouped;
        private List<string> sourceNodes;
        private string name;
        private StreamGraphNode node;

        public GroupedStreamAggregateBuilder(InternalStreamBuilder builder, Grouped<K, V> grouped, List<string> sourceNodes, string name, StreamGraphNode node)
        {
            this.builder = builder;
            this.grouped = grouped;
            this.sourceNodes = sourceNodes;
            this.name = name;
            this.node = node;
        }
    }
}
