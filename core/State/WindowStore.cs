using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// NOT IMPLEMENTED FOR MOMENT
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    public interface WindowStore<K,V> : IStateStore, ReadOnlyWindowStore<K,V>
    {
        void Put(K key, V value, long windowStartTimestamp);
    }
}
