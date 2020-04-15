using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Table.Internal
{
    internal interface IKTableValueGetterSupplier<K,V>
    {
        IKTableValueGetter<K, V> Get();

        String[] StoreNames { get; }
    }
}
