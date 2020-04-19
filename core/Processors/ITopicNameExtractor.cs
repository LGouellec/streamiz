using Kafka.Streams.Net.Processors.Internal;
using System;

namespace Kafka.Streams.Net.Processors
{
    /// <summary>
    /// An interface that allows to dynamically determine the name of the Kafka topic to send at the sink node of the topology.
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public interface ITopicNameExtractor<K, V>
    {
        /// <summary>
        /// Extracts the topic name to send to. The topic name must already exist, since the Kafka Streams library will not
        /// try to automatically create the topic with the extracted name.
        /// </summary>
        /// <param name="key">the record key</param>
        /// <param name="value">the record value</param>
        /// <param name="recordContext">current context metadata of the record</param>
        /// <returns>the topic name this record should be sent to</returns>
        String Extract(K key, V value, IRecordContext recordContext);
    }
}
