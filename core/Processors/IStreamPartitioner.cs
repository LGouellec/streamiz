using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors
{
    public interface IStreamPartitioner<K, V>
    {
        /// <summary>
        /// Function used to determine how records are distributed among partitions of the topic
        /// </summary>
        /// <param name="topic">Sink topic name</param>
        /// <param name="key">record's key</param>
        /// <param name="value">record's value</param>
        /// <param name="sourcePartition">record's source partition</param>
        /// <param name="numPartitions">number partitions of the sink topic</param>
        /// <returns>Return the destination partition for the current record</returns>
        Partition Partition(string topic, K key, V value, Partition sourcePartition, int numPartitions);
    }
}