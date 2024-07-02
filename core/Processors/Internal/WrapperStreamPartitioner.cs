using System;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class WrapperStreamPartitioner<K, V> : IStreamPartitioner<K, V>
    {
        private readonly Func<string, K, V, int, Partition> _partitioner;

        public WrapperStreamPartitioner(Func<string, K, V, int, Partition> partitioner)
        {
            _partitioner = partitioner;
        }

        public Partition Partition(string topic, K key, V value, int numPartitions)
            => _partitioner(topic, key, value, numPartitions);
    }
}