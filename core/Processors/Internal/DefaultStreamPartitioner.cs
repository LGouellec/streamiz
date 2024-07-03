using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// Forward the source partition as the sink partition of the record
    /// </summary>
    /// <typeparam name="K">Key record type</typeparam>
    /// <typeparam name="V">Value record type</typeparam>
    internal class DefaultStreamPartitioner<K, V> : IStreamPartitioner<K, V>
    {
        public Partition Partition(string topic, K key, V value, Partition sourcePartition, int numPartitions)
            => sourcePartition;
    }
}