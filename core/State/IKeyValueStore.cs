using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A key-value store that supports put/get/delete and range queries.
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public interface IKeyValueStore<K, V> : IStateStore, IReadOnlyKeyValueStore<K, V>
    {
        /// <summary>
        /// Update the value associated with this key.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value to update, it can be null if the serialized bytes are also null it is interpreted as deletes</param>
        void Put(K key, V value);

        /// <summary>
        /// Update the value associated with this key, unless a value is already associated with the key.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value to update, it can be null; if the serialized bytes are also null it is interpreted as deletes</param>
        /// <returns>The old value or null if there is no such key.</returns>
        V PutIfAbsent(K key, V value);

        /// <summary>
        /// Update all the given key/value pairs.
        /// </summary>
        /// <param name="entries">A list of entries to put into the store. if the serialized bytes are also null it is interpreted as deletes</param>
        void PutAll(IEnumerable<KeyValuePair<K, V>> entries);

        /// <summary>
        /// Delete the value from the store (if there is one).
        /// </summary>
        /// <param name="key">the key</param>
        /// <returns>The old value or null if there is no such key</returns>
        V Delete(K key);
    }
}
