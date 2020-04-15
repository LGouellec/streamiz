using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Streams.Net.SerDes;
using Kafka.Streams.Net.Stream.Internal.Graph.Nodes;

namespace Kafka.Streams.Net.Stream.Internal
{
    internal class KGroupedStream<K, V> : AbstractStream<K, V>, IKGroupedStream<K, V>
    {
        static String REDUCE_NAME = "KSTREAM-REDUCE-";
        static String AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

        public KGroupedStream(string name, Grouped<K,V> grouped, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder) 
            : base(name, grouped.Key, grouped.Value, sourceNodes, streamsGraphNode, builder)
        {
        }
    }
}
