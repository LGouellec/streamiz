using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class KGroupedStream<K, V> : AbstractStream<K, V>, IKGroupedStream<K, V>
    {
        //static string REDUCE_NAME = "KSTREAM-REDUCE-";
        //static string AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

        public KGroupedStream(string name, Grouped<K,V> grouped, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder) 
            : base(name, grouped.Key, grouped.Value, sourceNodes, streamsGraphNode, builder)
        {
        }
    }
}
