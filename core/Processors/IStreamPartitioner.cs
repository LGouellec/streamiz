using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors
{
    public interface IStreamPartitioner<K, V>
    {
        Partition Partition(string topic, K key, V value, Partition sourcePartition, int numPartitions);
    }
}