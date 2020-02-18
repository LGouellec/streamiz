using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Crosscutting
{
    internal static class LinqExtensions
    {
        internal static void AddRange<T>(this ISet<T> set, IEnumerable<T> values)
        {
            foreach (var v in values)
                set.Add(v);
        }

        internal static void RemoveAll<T>(this ISet<T> set, IEnumerable<T> values)
        {
            foreach (var v in values)
                set.Remove(v);
        }

        internal static V Get<K,V>(this IDictionary<K,V> map, K key)
        {
            if (map.ContainsKey(key))
                return map[key];
            else
                return default(V);
        }

        internal static void RemoveAll<K,V>(this IDictionary<K, V> map, IEnumerable<K> keys)
        {
            foreach (var k in keys)
                map.Remove(k);
        }

        internal static ISet<T> ToSet<T>(this IEnumerable<T> enumerable) => new HashSet<T>(enumerable);


    }
}
