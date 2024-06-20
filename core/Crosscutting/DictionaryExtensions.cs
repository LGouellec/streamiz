using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// Dictionary extensions
    /// </summary>
    public static class DictionaryExtensions
    {
        /// <summary>
        /// Add or Update element for the key.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="map">Source dictionary</param>
        /// <param name="key">New or updated key</param>
        /// <param name="value">Value</param>
        /// <returns>Return true if the key|value was added, false if updated</returns>
        public static bool AddOrUpdate<K, V>(this IDictionary<K, V> map, K key, V value)
        {
            if (map.ContainsKey(key))
            {
                map[key] = value;
                return false;
            }

            map.Add(key, value);
            return true;
        }

        /// <summary>
        /// Convert enumerable of <see cref="KeyValuePair{K, V}"/> to <see cref="IDictionary{K, V}"/>
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="source">Collection source</param>
        /// <returns>Return dictionary</returns>
        public static IDictionary<K, V> ToDictionary<K, V>(this IEnumerable<KeyValuePair<K, V>> source)
        {
            Dictionary<K, V> r = new Dictionary<K, V>();
            foreach (var s in source)
                r.Add(s.Key, s.Value);
            return r;
        }

        
        /// <summary>
        /// Convert enumerable of <see cref="IEnumerable{T}"/> to <see cref="IDictionary{K, V}"/>.
        /// If a key already exists, value will be replace.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="elementSelector"></param>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <returns></returns>
        public static IDictionary<K, V> ToUpdateDictionary<T, K, V>(
            this IEnumerable<T> source,
            Func<T, K> keySelector,
            Func<T, V> elementSelector)
        {
            var dictonary = new Dictionary<K, V>();
            foreach (var element in source)
            {
                var key = keySelector(element);
                if (dictonary.ContainsKey(key))
                    dictonary[key] = elementSelector(element);
                else
                    dictonary.Add(key, elementSelector(element));
            }

            return dictonary;
        }

        /// <summary>
        /// Merge an other dictionary into map dictionary
        /// </summary>
        /// <param name="map">source dictionary</param>
        /// <param name="secondMap">dictionary to merge</param>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <returns>Return map dictionary merged with secondMap</returns>
        public static IDictionary<K, V> AddRange<K, V>(this IDictionary<K, V> map, IDictionary<K, V> secondMap)
        {
            if(secondMap != null)
                foreach (var entry in secondMap)
                    map.AddOrUpdate(entry.Key, entry.Value);
            return map;
        }

        /// <summary>
        /// Try add or update a key/value on a <see cref="ConcurrentDictionary{K,V}"/>
        /// </summary>
        /// <param name="key">key to add or update</param>
        /// <param name="value">new value</param>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="source">Collection source</param>
        /// <returns>Return true if the value is added, false otherwise</returns>
        public static bool TryAddOrUpdate<K, V>(this ConcurrentDictionary<K, V> source, K key, V value)
        {
            V valueTmp;
            if (source.ContainsKey(key))
            {
                if (source.TryGetValue(key, out valueTmp))
                    source.TryUpdate(key, value, valueTmp);
                return false;
            }   
            else
            {
                return source.TryAdd(key, value);
            }
        }

        /// <summary>
        /// Create list in the value or append the existing list
        /// </summary>
        /// <param name="source">Collection source</param>
        /// <param name="key">key to add or update</param>
        /// <param name="value">new value</param>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        public static void CreateListOrAdd<K, V>(this IDictionary<K, List<V>> source, K key, V value)
        {
            if(source.ContainsKey(key))
                source[key].Add(value);
            else
                source.Add(key, new List<V>{value});
        }
        
       #if NETSTANDARD2_0
        public static bool TryAdd<K, V>( this IDictionary<K, V> dictionary,
            K key,
            V value){
            if (dictionary == null)
                throw new ArgumentNullException(nameof (dictionary));
            if (dictionary.ContainsKey(key))
                return false;
            dictionary.Add(key, value);
            return true;
            }
        
         public static bool Remove<K, V>( this IDictionary<K, V> dictionary,
            K key,
            out V value){
             bool result = dictionary.TryGetValue(key, out V valueTmp);
             if (result)
             {
                 value = valueTmp;
                 dictionary.Remove(key);
                 return true;
             }
             value = default(V);
             return false;
         }
        #endif
        
    }
}
