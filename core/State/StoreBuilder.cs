using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Build a <see cref="IStateStore"/> wrapped with optional caching and logging.
    /// </summary>
    public interface StoreBuilder
    {
        bool IsWindowStore { get; }
        long RetentionMs { get; }
        IDictionary<string, string> LogConfig { get; }
        bool LoggingEnabled { get; }
        string Name { get; }
        IStateStore Build();
    }

    /// <summary>
    /// Build a <see cref="IStateStore"/> wrapped with optional caching and logging.
    /// </summary>
    /// <typeparam name="T">the type of store to build</typeparam>
    public interface StoreBuilder<T>  : StoreBuilder
        where T : IStateStore
    {
        StoreBuilder<T> WithCachingEnabled();
        StoreBuilder<T> WithCachingDisabled();
        StoreBuilder<T> WithLoggingEnabled(IDictionary<String, String> config);
        StoreBuilder<T> WithLoggingDisabled();
        new T Build();
    }
}
