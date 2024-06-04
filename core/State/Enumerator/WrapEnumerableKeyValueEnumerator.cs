using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class WrapEnumerableKeyValueEnumerator<K, V> :
        IKeyValueEnumerator<K, V>
    {
        private readonly IEnumerable<KeyValuePair<K, V>> _enumerable;
        private IEnumerator<KeyValuePair<K, V>> _enumerator;
        private KeyValuePair<K, V>? current = null;

        public WrapEnumerableKeyValueEnumerator(IEnumerable<KeyValuePair<K, V>> enumerable)
        {
            _enumerable = enumerable;
            _enumerator = enumerable.GetEnumerator();
        }

        public KeyValuePair<K, V>? Current => current;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            _enumerator.Dispose();
        }

        public bool MoveNext()
        {
            var result = _enumerator.MoveNext();
            if (result)
                current = new KeyValuePair<K, V>(_enumerator.Current.Key, _enumerator.Current.Value);
            else
                current = null;
            return result;
        }

        public K PeekNextKey()
            => current != null ? current.Value.Key : default(K);

        public void Reset()
        {
            _enumerator.Dispose();
            current = null;
            _enumerator = _enumerable.GetEnumerator();
        }
    }
}