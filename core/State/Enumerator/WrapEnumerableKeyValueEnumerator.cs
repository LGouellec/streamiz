using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class WrapEnumerableKeyValueEnumerator<K, V> :
        IKeyValueEnumerator<K, V>
    {
        private readonly List<KeyValuePair<K, V>> values;
        private int index = 0;
        private KeyValuePair<K, V>? current = null;

        public WrapEnumerableKeyValueEnumerator(IEnumerable<KeyValuePair<K, V>> enumerable)
        {
            values = enumerable.ToList();
        }

        public KeyValuePair<K, V>? Current => current;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            current = null;
            index = 0;
        }

        public bool MoveNext()
        {
            if (values.Count > 0 && index < values.Count)
            {
                current = values[index];
                ++index;
                return true;
            }
            else
                return false;
        }

        public K PeekNextKey()
            => current != null ? current.Value.Key : default(K);

        public void Reset()
        {
            index = 0;
            current = null;
        }
    }
}