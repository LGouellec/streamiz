using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State
{
    public interface SessionStore<K,AGG> : IStateStore, ReadOnlySessionStore<K,AGG>
    {
    }
}
