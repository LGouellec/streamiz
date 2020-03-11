using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    public interface SessionStore<K,AGG> : StateStore, ReadOnlySessionStore<K,AGG>
    {
    }
}
