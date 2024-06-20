using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class EmptyKeyValueEnumerator<K, V> : IKeyValueEnumerator<K, V>
    {
        public static EmptyKeyValueEnumerator<K, V> Empty => new();
        
        public KeyValuePair<K, V>? Current => null;

        object IEnumerator.Current => null;

        public void Dispose()
        {

        }

        public bool MoveNext() => false;

        public K PeekNextKey() => default;

        public void Reset() { }
    }

    internal class EmptyWindowStoreEnumerator<V> : IWindowStoreEnumerator<V>
    {
        public KeyValuePair<long, V>? Current => null;

        object IEnumerator.Current => null;

        public void Dispose()
        {
        }

        public bool MoveNext() => false;


        public long PeekNextKey() => 0L;

        public void Reset()
        {
        }
    }
}
