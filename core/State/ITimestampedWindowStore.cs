using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Interface for storing the aggregated values of fixed-size time windows.
    /// <p>
    /// In contrast to a <see cref="IWindowStore{K, V}"/> that stores plain windowedKeys-value pairs,
    /// a <see cref="ITimestampedWindowStore{K, V}"/> stores windowedKeys-(value/timestamp) pairs.
    /// </p>
    /// While the window start- and end-timestamp are fixed per window, the value-side timestamp is used
    /// to store the last update timestamp of the corresponding window.
    /// </summary>
    /// <typeparam name="K">Type of keys</typeparam>
    /// <typeparam name="V">Type of values</typeparam>
    public interface ITimestampedWindowStore<K, V> : IWindowStore<K, ValueAndTimestamp<V>>, ITimestampedStore
    {
    }
}
