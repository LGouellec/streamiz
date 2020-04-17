using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Crosscutting
{
    /// <summary>
    /// Dictionnary extensions
    /// </summary>
    public static class DictionaryExtensions
    {
        /// <summary>
        /// Add or Update element for the key.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="map">Source dictionnary</param>
        /// <param name="key">New or updated key</param>
        /// <param name="value">Value</param>
        /// <returns>Return true if the key|value was added, false if updated</returns>
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

        /// <summary>
        /// Convert enumerable of <see cref="KeyValuePair{K, V}"/> to <see cref="IDictionary{K, V}"/>
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="source">Collection source</param>
        /// <returns>Return dictionnary</returns>
        public static IDictionary<K, V> ToDictionary<K, V>(this IEnumerable<KeyValuePair<K, V>> source)
        {
            Dictionary<K, V> r = new Dictionary<K, V>();
            foreach (var s in source)
                r.Add(s.Key, s.Value);
            return r;
        }
    }
}
