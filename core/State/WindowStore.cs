using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    public interface WindowStore<K,V> : IStateStore, ReadOnlyWindowStore<K,V>
    {
    }
}
