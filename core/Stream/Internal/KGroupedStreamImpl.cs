using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream.Internal.Graph.Nodes;

namespace kafka_stream_core.Stream.Internal
{
    internal class KGroupedStreamImpl<K, V> : AbstractStream<K, V>, KGroupedStream<K, V>
    {
        static String REDUCE_NAME = "KSTREAM-REDUCE-";
        static String AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

        public KGroupedStreamImpl(string name, Grouped<K,V> grouped, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder) 
            : base(name, grouped.Key, grouped.Value, sourceNodes, streamsGraphNode, builder)
        {
        }
    }
}
