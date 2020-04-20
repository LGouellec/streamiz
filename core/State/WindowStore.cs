using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State
{
    public interface WindowStore<K,V> : IStateStore, ReadOnlyWindowStore<K,V>
    {
    }
}
