using System;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class WrapperStreamPartitioner<K, V> : IStreamPartitioner<K, V>
    {
        private readonly Func<string, K, V, Partition, int, Partition> _partitioner;

        public WrapperStreamPartitioner(Func<string, K, V, Partition, int, Partition> partitioner)
        {
            _partitioner = partitioner;
        }

        public Partition Partition(string topic, K key, V value, Partition sourcePartition, int numPartitions)
            => _partitioner(topic, key, value, sourcePartition, numPartitions);
    }
}