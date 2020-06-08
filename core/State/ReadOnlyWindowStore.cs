using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// NOT IMPLEMENTED FOR MOMENT
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    public interface ReadOnlyWindowStore<K,V>
    {
        V Fetch(K key, long time);

        IEnumerator<V> Fetch(K key, DateTime from, DateTime to);

        IEnumerator<Tuple<Windowed<K>, V>> All();

        IEnumerator<Tuple<Windowed<K>, V>> FetchAll(DateTime from, DateTime to);
    }
}
