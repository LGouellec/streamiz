using kafka_stream_core.SerDes;

namespace kafka_stream_core.State
{
    public interface TimestampedKeyValueStore<K,V> : KeyValueStore<K, ValueAndTimestamp<V>>
    {
    }
}
