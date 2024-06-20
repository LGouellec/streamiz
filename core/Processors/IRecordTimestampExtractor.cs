namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// An interface that allows to dynamically determine the timestamp of the record stored in the Kafka topic.
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public interface IRecordTimestampExtractor<K,V>
    {
        /// <summary>
        /// Extracts the timestamp of the record stored in the Kafka topic.
        /// </summary>
        /// <param name="key">the record key</param>
        /// <param name="value">the record value</param>
        /// <param name="recordContext">current context metadata of the record</param>
        /// <returns>the timestamp of the record</returns>
        long Extract(K key, V value, IRecordContext recordContext);

    }
}
