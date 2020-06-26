using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    public interface IKeyValueEnumerator<K, V> : IEnumerator<KeyValuePair<K, V>?>
    {
        K PeekNextKey();
    }
}
