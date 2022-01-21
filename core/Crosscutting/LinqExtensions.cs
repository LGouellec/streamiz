using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal static class LinqExtensions
    {
        internal static void RemoveAll<T>(this IList<T> list, IEnumerable<T> toRemove)
        {
            foreach (var t in toRemove)
                list.Remove(t);
        }
        
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
            return obj != null ? new List<T> { obj } : new List<T>();
        }
    
        internal static IList<T> AddIfNotNull<T>(this IList<T> list, T data)
        {
            if (data != null)
                list.Add(data);
            return list;
        }
    
        internal static void ForEach<T>(this ICollection<T> collections, Action<T> action)
        {
            foreach (var i in collections)
                action(i);
        }

        internal static bool ContainsAll<T>(this IEnumerable<T> source, IEnumerable<T> sink)
        {
            bool res = true;
            foreach (var t in source)
            {
                res = sink.Contains(t);
                if (!res)
                    return false;
            }

            return true;
        }
    }
}