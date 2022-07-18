using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// External record during an asynchronous processing operation. It's wrap the key/value/headers and timestamp.
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public class ExternalRecord<K, V>
    {
        /// <summary>
        /// Current record key
        /// </summary>
        public K Key { get; }
        /// <summary>
        /// Current record value
        /// </summary>
        public V Value { get; }
        /// <summary>
        /// Current record headers
        /// </summary>
        public IReadOnlyCollection<IHeader> Headers { get; }
        /// <summary>
        /// Current record timestamp
        /// </summary>
        public long Timestamp { get; }
        
        internal ExternalRecord(
            K key,
            V value,
            Headers headers,
            long timestamp)
        {
            Value = value;
            Timestamp = timestamp;
            Key = key;
            Headers = new ReadOnlyCollection<IHeader>(headers != null ? headers.ToList() : new List<IHeader>());
        }
    }
}