using System;

namespace kafka_stream_core.Processors
{
    internal interface IStreamPartitioner<K, V>
    {
        int Partition(String topic, K key, V value, int numPartitions);
    }
}
