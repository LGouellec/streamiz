using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal static class SortedDictionaryExtensions
    {
        internal static IEnumerable<KeyValuePair<K, V>> HeadMap<K, V>(this IEnumerable<KeyValuePair<K, V>> enumerable,  K key, bool inclusive)
            where K : IComparable<K>
        {
            foreach (var kv in enumerable)
            {
                int r = key.CompareTo(kv.Key);
                if ((inclusive && r >= 0) || (!inclusive && r > 0))
                    yield return new KeyValuePair<K, V>(kv.Key, kv.Value);
                else
                    break;
            }
        }

        internal static IEnumerable<KeyValuePair<K, V>> SubMap<K, V>(this IEnumerable<KeyValuePair<K, V>> enumerable, K keyFrom, K keyTo , bool inclusiveFrom, bool inclusiveTo)
            where K : IComparable<K>
        {
            foreach (var kv in enumerable)
            {
                int rF = keyFrom.CompareTo(kv.Key);
                int rT = keyTo.CompareTo(kv.Key);

                if((inclusiveFrom && rF <= 0) || (!inclusiveFrom && rF < 0))
                {
                    if ((inclusiveTo && rT >= 0) || (!inclusiveTo && rT > 0))
                    {
                        yield return new KeyValuePair<K, V>(kv.Key, kv.Value);
                    }
                    else
                        break;
                }
            }
        }

        internal static IEnumerable<KeyValuePair<K, V>> TailMap<K, V>(this IEnumerable<KeyValuePair<K, V>> enumerable, K keyFrom,
            bool inclusive)
            where K : IComparable<K>
        {
            foreach (var kv in enumerable)
            {
                int rT = keyFrom.CompareTo(kv.Key);

                if ((inclusive && rT <= 0) || (!inclusive && rT < 0))
                {
                    yield return new KeyValuePair<K, V>(kv.Key, kv.Value);
                }
            }
        }
    }
}
