using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Crosscutting
{
    public static class DictionaryExtensions
    {
        public static void AddOrUpdate<K,V>(this IDictionary<K,V> map, K key, V value)
        {
            if (map.ContainsKey(key))
                map[key] = value;
            else
                map.Add(key, value);
        }
    }
}
