using Confluent.Kafka;
using Kafka.Streams.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Processors.Internal
{
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
