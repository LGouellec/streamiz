using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    public interface IWindowStoreEnumerator<V> : IKeyValueEnumerator<long, V>
    {
    }
}
