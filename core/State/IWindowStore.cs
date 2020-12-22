using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Interface for storing the aggregated values of fixed-size time windows.
    /// </summary>
    /// <typeparam name="K">Type of keys</typeparam>
    /// <typeparam name="V">Type of values</typeparam>
    public interface IWindowStore<K,V> : IStateStore, IReadOnlyWindowStore<K,V>
    {
        /// <summary>
        /// Put a key-value pair into the window with given window start timestamp.
        /// If serialized value bytes are null it is interpreted as delete. Note that deletes will be
        /// ignored in the case of an underlying store that retains duplicates.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value; can be null</param>
        /// <param name="windowStartTimestamp">The timestamp of the beginning of the window to put the key/value into</param>
        void Put(K key, V value, long windowStartTimestamp);
    }
}
