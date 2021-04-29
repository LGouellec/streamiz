using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class WrapEnumerableKeyValueEnumerator<K, V> :
        IKeyValueEnumerator<K, V>
    {
        private readonly IEnumerator<KeyValuePair<K, V>> wrappedEnumerator;

        public WrapEnumerableKeyValueEnumerator(IEnumerable<KeyValuePair<K, V>> enumerable)
        {
            wrappedEnumerator = enumerable.GetEnumerator();
        }

        public KeyValuePair<K, V>? Current => wrappedEnumerator.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
            => wrappedEnumerator.Dispose();

        public bool MoveNext()
            => wrappedEnumerator.MoveNext();

        public K PeekNextKey()
            => wrappedEnumerator.Current.Key;

        public void Reset()
            => wrappedEnumerator.Reset();
    }
}