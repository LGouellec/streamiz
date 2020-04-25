using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A key-value store that only supports read operations.
    /// Implementations should be thread-safe as concurrent reads and writes are expected.
    /// Please note that this contract defines the thread-safe read functionality only; it does not
    /// guarantee anything about whether the actual instance is writable by another thread, or
    /// whether it uses some locking mechanism under the hood. For this reason, making dependencies
    /// between the read and write operations on different StateStore instances can cause concurrency
    /// problems like deadlock.
    /// </summary>
    /// <typeparam name="K">the key type</typeparam>
    /// <typeparam name="V">the value type</typeparam>
    public interface ReadOnlyKeyValueStore<K, V>
    {
        /// <summary>
        /// Get the value corresponding to this key.
        /// </summary>
        /// <param name="key">the key to fetch</param>
        /// <returns>The value or null if no value is found.</returns>
        V Get(K key);

        // TODO : 
        //KeyValueIterator<K, V> range(K from, K to);

        /// <summary>
        /// Return an iterator over all keys in this store. No ordering guarantees are provided.
        /// </summary>
        /// <returns>An iterator of all key/value pairs in the store.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        IEnumerable<KeyValuePair<K, V>> All();

        /// <summary>
        /// Return an approximate count of key-value mappings in this store.
        /// The count is not guaranteed to be exact in order to accommodate stores
        /// where an exact count is expensive to calculate.
        /// </summary>
        /// <returns>an approximate count of key-value mappings in the store.</returns>
        long ApproximateNumEntries();
    }
}
