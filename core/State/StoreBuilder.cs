using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    public interface StoreBuilder<T> 
        where T : StateStore
    {
        IDictionary<String, String> LogConfig { get; }
        bool LoggingEnabled { get; }
        String Name { get; }

        StoreBuilder<T> withCachingEnabled();
        StoreBuilder<T> withCachingDisabled();
        StoreBuilder<T> withLoggingEnabled(IDictionary<String, String> config);
        StoreBuilder<T> withLoggingDisabled();
        T build();
    }
}
