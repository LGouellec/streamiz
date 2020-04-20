using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State
{
    public interface TimestampedKeyValueStore<K,V> : KeyValueStore<K, ValueAndTimestamp<V>>
    {
    }
}
