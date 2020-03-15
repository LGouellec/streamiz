using kafka_stream_core.State;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal
{
    internal interface IKTableValueGetter<K,V>
    {
        void init(ProcessorContext context);

        ValueAndTimestamp<V> get(K key);

        void close();
    }
}
