using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal
{
    internal class KGroupedTableImpl<K, V> : AbstractStream<K, V>, KGroupedTable<K, V>
    {
        public KGroupedTableImpl(string name, Grouped<K, V> grouped, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder) 
            : base(name, grouped.Key, grouped.Value, sourceNodes, streamsGraphNode, builder)
        {
        }
    }
}
