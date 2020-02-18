using System;

namespace kafka_stream_core.Processors
{
    internal interface StreamPartitioner<K, V>
    {
        int partition(String topic, K key, V value, int numPartitions);
    }
}
