using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KGroupedTable<K, V> : AbstractStream<K, V>, IKGroupedTable<K, V>
    {
        public KGroupedTable(string name, Grouped<K, V> grouped, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder) 
            : base(name, grouped.Key, grouped.Value, sourceNodes, streamsGraphNode, builder)
        {
        }
    }
}
