using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface IStreamPartitioner<K, V>
    {
        int Partition(String topic, K key, V value, int numPartitions);
    }
}
