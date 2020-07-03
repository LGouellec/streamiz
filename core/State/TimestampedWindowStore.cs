namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Interface for storing the aggregated values of fixed-size time windows.
    /// <p>
    /// In contrast to a <see cref="WindowStore{K, V}"/> that stores plain windowedKeys-value pairs,
    /// a <see cref="TimestampedWindowStore{K, V}"/> stores windowedKeys-(value/timestamp) pairs.
    /// </p>
    /// While the window start- and end-timestamp are fixed per window, the value-side timestamp is used
    /// to store the last update timestamp of the corresponding window.
    /// </summary>
    /// <typeparam name="K">Type of keys</typeparam>
    /// <typeparam name="V">Type of values</typeparam>
    public interface TimestampedWindowStore<K, V> : WindowStore<K, ValueAndTimestamp<V>>
    {
    }
}
