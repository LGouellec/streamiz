namespace kafka_stream_core.Stream
{
    public interface IKeyValueMapper<K, V, VR>
    {
        VR apply(K key, V value);
    }
}
