using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    public class WrapKeyValueEnumeratorEnumerable<K, V> 
        : IEnumerable<KeyValuePair<K, V>>
    {
        private readonly IKeyValueEnumerator<K, V> _keyValueEnumerator;

        public WrapKeyValueEnumeratorEnumerable(IKeyValueEnumerator<K, V> keyValueEnumerator)
        {
            _keyValueEnumerator = keyValueEnumerator;
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
            => _keyValueEnumerator as IEnumerator<KeyValuePair<K, V>>;

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}