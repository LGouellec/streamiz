using Kafka.Streams.Net.SerDes;
using Kafka.Streams.Net.Stream;
using Kafka.Streams.Net.Stream.Internal;
using Kafka.Streams.Net.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Table.Internal
{
    internal class KGroupedTable<K, V> : AbstractStream<K, V>, IKGroupedTable<K, V>
    {
        public KGroupedTable(string name, Grouped<K, V> grouped, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder) 
            : base(name, grouped.Key, grouped.Value, sourceNodes, streamsGraphNode, builder)
        {
        }
    }
}
