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

    // NOTE: in Java implementation ReadOnlyKeyValueStore does not implement ReadOnlyKeyValueStore
    // I guess it is a mistake. Without this 
    public interface ReadOnlyKeyValueStore<K, V>//: IStateStore
    {
        /// <summary>
        /// Get the value corresponding to this key.
        /// </summary>
        /// <param name="key">the key to fetch</param>
        /// <returns>The value or null if no value is found.</returns>
        V Get(K key);

        // TODO : 
        //KeyValueIterator<K, V> range(K from, K to);

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
