using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Crosscutting
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

        internal static IEnumerable<T> Except<T>(this IEnumerable<T> source, Func<T, bool> function)
        {
            foreach (var item in source)
                if (!function.Invoke(item))
                    yield return item;
        }

        internal static IEnumerable<KeyValuePair<K, V>> Intercept<K,V>(this IEnumerable<KeyValuePair<K, V>> source, Func<KeyValuePair<K, V>, bool> function, V interceptedValue)
        {
            List<KeyValuePair<K, V>> results = new List<KeyValuePair<K, V>>();
            foreach (var item in source)
            {
                if (function.Invoke(item))
                    results.Add(KeyValuePair.Create(item.Key, interceptedValue));
                else
                    results.Add(item);
            }
            return results;
        }

        internal static IEnumerable<T> ToSingle<T>(this T obj)
        {
            return new List<T> { obj };
        }
    }
}
