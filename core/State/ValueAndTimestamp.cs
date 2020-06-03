namespace Streamiz.Kafka.Net.State
{
    internal class ValueAndTimestamp
    {
        public static V GetValueOrNull<V>(ValueAndTimestamp<V> valueAndTimestamp)
        {
            return valueAndTimestamp == null ? default : valueAndTimestamp.Value;
        }
    }

    /// <summary>
    /// Combines a value from a keyvalue with a timestamp.
    /// </summary>
    /// <typeparam name="V">Value type</typeparam>
    public class ValueAndTimestamp<V>
    {
        /// <summary>
        /// Return the wrapped value
        /// </summary>
        public V Value { get; private set; }

        /// <summary>
        /// Return the wrapped timestamp of value
        /// </summary>
        public long Timestamp { get; private set; }

        private ValueAndTimestamp(long timestamp, V value)
        {
            this.Timestamp = timestamp;
            this.Value = value;
        }

        /// <summary>
        /// Create a new <see cref="ValueAndTimestamp{V}"/> instance if the provide <code>value</code> is not null.
        /// </summary>
        /// <param name="value">the value</param>
        /// <param name="timestamp">the timestamp</param>
        /// <returns>a new <see cref="ValueAndTimestamp{V}"/> instance if the provide <code>value</code> is not null; otherwise null is returned</returns>
        public static ValueAndTimestamp<V> Make(V value, long timestamp)
        {
            return value == null ? null : new ValueAndTimestamp<V>(timestamp, value);
        }
    }
}
