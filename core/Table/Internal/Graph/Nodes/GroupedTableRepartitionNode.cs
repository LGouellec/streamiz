using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SerDes.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Table.Internal.Graph.Nodes
{
    internal class GroupedTableRepartitionNode<K, V> : RepartitionNode<K, Change<V>>
    {
        public GroupedTableRepartitionNode(string streamGraphNode, string sourceName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string sinkName, string repartitionTopic) 
            : base(streamGraphNode, sourceName, null, keySerdes, new ChangeSerDes<V>(valueSerdes), sinkName, repartitionTopic)
        {
        }
    }
}