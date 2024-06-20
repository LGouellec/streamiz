using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Cache
{
    internal interface ICachedStateStore<K, V>
    {
        bool SetFlushListener(Action<KeyValuePair<K, Change<V>>> listener, bool sendOldChanges);
    }
}