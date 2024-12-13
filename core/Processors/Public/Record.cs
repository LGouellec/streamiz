using Confluent.Kafka;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// Public record kafka
    /// </summary>
    /// <typeparam name="K">type of the key</typeparam>
    /// <typeparam name="V">type of the value</typeparam>
    public class Record<K, V>
    {
        /// <summary>
        /// Topic partition offset of the record (readonly)
        /// </summary>
        public TopicPartitionOffset TopicPartitionOffset { get; }
        
        /// <summary>
        /// Headers of the record (readonly)
        /// </summary>
        public Headers Headers { get; }
        
        /// <summary>
        /// Timestamp of the record (readonly)
        /// </summary>
        public Timestamp Timestamp { get; }
        
        /// <summary>
        /// Current key of the record
        /// </summary>
        public K Key { get; }
        
        /// <summary>
        /// Current value of the record
        /// </summary>
        public V Value { get; }

        internal Record(TopicPartitionOffset topicPartitionOffset, Headers headers, Timestamp timestamp, K key, V value)
            : this(key, value, headers)
        {
            TopicPartitionOffset = topicPartitionOffset;
            Timestamp = timestamp;
        }

        private Record(K key, V value)
            : this(value)
        {
            Key = key;
        }
        
        private Record(K key, V value, Headers headers)
            : this(key, value)
        {
            Headers = headers;
        }

        private Record(V value) : this(value, new Headers())
        { }
        
        private Record(V value, Headers headers)
        {
            Value = value;
            Headers = headers;
        }

        /// <summary>
        /// Use this helper method for returning a new record (key/value) with <see cref="IKStream{K,V}.Transform{K1,V1}"/>
        /// </summary>
        /// <param name="key">Key of the record</param>
        /// <param name="value">Value of the record</param>
        /// <returns>return a new key/value record</returns>
        public static Record<K, V> Create(K key, V value)
            => new(key, value);

        /// <summary>
        /// Use this helper method for returning a new record (value) with <see cref="IKStream{K,V}.TransformValues{V1}"/>
        /// </summary>
        /// <param name="value">Value of the record</param>
        /// <returns>return a new value record</returns>
        public static Record<K, V> Create(V value)
            => new(value);
        
        /// <summary>
        /// Use this helper method for returning a new record (value) and headers with <see cref="IKStream{K,V}.TransformValues{V1}"/>
        /// </summary>
        /// <param name="value">Value of the record</param>
        /// <param name="headers">Headers of the record</param>
        /// <returns>return a new value record</returns>
        public static Record<K, V> Create(V value, Headers headers)
            => new(value, headers);

        /// <summary>
        /// Use this helper method for returning a new record (key/value) and headers with <see cref="IKStream{K,V}.Transform{K1,V1}"/>
        /// </summary>
        /// <param name="key">Key of the record</param>
        /// <param name="value">Value of the record</param>
        /// <param name="headers">Headers of the record</param>
        /// <returns>return a new key/value record</returns>
        public static Record<K, V> Create(K key, V value, Headers headers)
            => new(key, value, headers);
    }
}