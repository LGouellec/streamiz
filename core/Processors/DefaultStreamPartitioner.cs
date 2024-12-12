using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Forward the source partition as the sink partition of the record if there is enough sink partitions, Partition.Any otherwise
    /// </summary>
    /// <typeparam name="K">Key record type</typeparam>
    /// <typeparam name="V">Value record type</typeparam>
    public class DefaultStreamPartitioner<K, V> : IStreamPartitioner<K, V>
    {
        /// <summary>
        /// Function used to determine how records are distributed among partitions of the topic
        /// </summary>
        /// <param name="topic">Sink topic name</param>
        /// <param name="key">record's key</param>
        /// <param name="value">record's value</param>
        /// <param name="sourcePartition">record's source partition</param>
        /// <param name="numPartitions">number partitions of the sink topic</param>
        /// <returns>Return the source partition as the sink partition of the record if there is enough sink partitions, Partition.Any otherwise</returns>
        public Partition Partition(string topic, K key, V value, Partition sourcePartition, int numPartitions)
        {
            if (sourcePartition.Value <= numPartitions - 1)
                return sourcePartition;
            return Confluent.Kafka.Partition.Any;
        }
    }
}