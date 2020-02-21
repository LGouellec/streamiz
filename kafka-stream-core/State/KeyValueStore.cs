using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    public interface KeyValueStore<K, V> : StateStore, ReadOnlyKeyValueStore<K, V>
    {
        void put(K key, V value);

        V putIfAbsent(K key, V value);

        void putAll(IEnumerable<KeyValuePair<K, V>> entries);

        V delete(K key);
    }
}
