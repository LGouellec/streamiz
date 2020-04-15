using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Crosscutting
{
    public static class DictionaryExtensions
    {
        public static bool AddOrUpdate<K,V>(this IDictionary<K,V> map, K key, V value)
        {
            if (map.ContainsKey(key))
            {
                map[key] = value;
                return false;
            }
            else
            {
                map.Add(key, value);
                return true;
            }
        }
    
        public static IDictionary<K, V> ToDictionary<K, V>(this IEnumerable<KeyValuePair<K, V>> source)
        {
            Dictionary<K, V> r = new Dictionary<K, V>();
            foreach (var s in source)
                r.Add(s.Key, s.Value);
            return r;
        }
    }
}
