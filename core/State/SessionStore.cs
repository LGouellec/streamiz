using Kafka.Streams.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State
{
    public interface SessionStore<K,AGG> : IStateStore, ReadOnlySessionStore<K,AGG>
    {
    }
}
