using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal
{
    internal interface IKTableValueGetterSupplier<K,V>
    {
        IKTableValueGetter<K, V> get();

        String[] StoreNames { get; }
    }
}
