using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class DefaultStreamPartitioner<K, V> : IStreamPartitioner<K, V>
    {
        public Partition Partition(string topic, K key, V value, int numPartitions)
            => Confluent.Kafka.Partition.Any;
    }
}