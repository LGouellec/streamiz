namespace Streamiz.Kafka.Net.State
{
    public interface TimestampedWindowStore<K, V> : WindowStore<K, ValueAndTimestamp<V>>
    {
    }
}
