using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Extensions of custom enumerator. 
    /// e.g <see cref="IKeyValueEnumerator{K, V}"/>
    /// </summary>
    public static class EnumeratorExtensions
    {
        /// <summary>
        /// Creates a <see cref="List{KeyValuePair}"/> from an <see cref="IKeyValueEnumerator{K, V}"/>
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="enumerator"><see cref="IKeyValueEnumerator{K, V}"/> enumerator</param>
        /// <returns>Return an instance of <see cref="List{KeyValuePair}"/></returns>
        public static List<KeyValuePair<K, V>> ToList<K, V>(this IKeyValueEnumerator<K, V> enumerator)
        {
            List<KeyValuePair<K, V>> list = new List<KeyValuePair<K, V>>();

            while (enumerator.MoveNext())
            {
                if (enumerator.Current.HasValue)
                {
                    list.Add(enumerator.Current.Value);
                }
            }

            enumerator.Dispose();
            return list;
        }
    }
}
