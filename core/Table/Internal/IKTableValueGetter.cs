using kafka_stream_core.State;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal
{
    internal interface IKTableValueGetter<K,V>
    {
        void Init(ProcessorContext context);

        ValueAndTimestamp<V> Get(K key);

        void Close();
    }
}
