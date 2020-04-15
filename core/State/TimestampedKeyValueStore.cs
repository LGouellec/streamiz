using Kafka.Streams.Net.SerDes;

namespace Kafka.Streams.Net.State
{
    public interface TimestampedKeyValueStore<K,V> : KeyValueStore<K, ValueAndTimestamp<V>>
    {
    }
}
