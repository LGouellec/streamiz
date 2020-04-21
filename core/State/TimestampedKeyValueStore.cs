namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A key-(value/timestamp) store that supports put/get/delete and range queries.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    public interface TimestampedKeyValueStore<K,V> : IKeyValueStore<K, ValueAndTimestamp<V>>
    {
    }
}
