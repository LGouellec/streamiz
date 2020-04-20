using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    // TODO : Not used for moment
    // FOR MOMENT, impossible to set customer partitionner with Confluent.Kafka library
    // FOR TRACKING : https://github.com/confluentinc/confluent-kafka-dotnet/issues/587
    internal class DefaultStreamPartitioner<K,V> : IStreamPartitioner<K,V>
    {
        private readonly Metadata cluster;
        // TODO : Use Kafka Serializer with context
        private readonly ISerDes<K> keySerializer;
        // TODO : use default partitioner .net
        //private readonly DefaultPartitioner defaultPartitioner;

        public DefaultStreamPartitioner(ISerDes<K> keySerializer, Metadata cluster)
        {
            this.cluster = cluster;
            this.keySerializer = keySerializer;
            //this.defaultPartitioner = new DefaultPartitioner();
        }

        public ISerDes<K> KeySerializer => keySerializer;

        public int Partition(string topic, K key, V value, int numPartitions)
        {
            // TODO
            return 1;
            //byte[] keyBytes = keySerializer.Serialize(topic, key);
            //return defaultPartitioner.partition(topic, key, keyBytes, value, null, cluster);
        }
    }
}
