using Confluent.Kafka;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors.Internal
{
    internal class DefaultStreamPartitioner<K,V> : StreamPartitioner<K,V>
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

        public int partition(string topic, K key, V value, int numPartitions)
        {
            // TODO
            return 1;
            //byte[] keyBytes = keySerializer.Serialize(topic, key);
            //return defaultPartitioner.partition(topic, key, keyBytes, value, null, cluster);
        }
    }
}
