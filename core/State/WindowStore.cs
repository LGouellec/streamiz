using Kafka.Streams.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State
{
    public interface WindowStore<K,V> : IStateStore, ReadOnlyWindowStore<K,V>
    {
    }
}
