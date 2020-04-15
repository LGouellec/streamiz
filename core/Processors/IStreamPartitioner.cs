using System;

namespace Kafka.Streams.Net.Processors
{
    internal interface IStreamPartitioner<K, V>
    {
        int Partition(String topic, K key, V value, int numPartitions);
    }
}
