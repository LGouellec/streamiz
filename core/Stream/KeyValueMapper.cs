namespace kafka_stream_core.Stream
{
    public interface KeyValueMapper<K, V, VR>
    {
        VR apply(K key, V value);
    }
}
