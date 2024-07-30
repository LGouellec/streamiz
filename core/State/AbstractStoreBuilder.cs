using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Build a <see cref="IStateStore"/> wrapped with optional caching and logging.
    /// </summary>
    /// <typeparam name="K">type of the key</typeparam>
    /// <typeparam name="V">type of the value</typeparam>
    /// <typeparam name="T">the type of store to build</typeparam>
    public abstract class AbstractStoreBuilder<K, V, T> : IStoreBuilder<T>
        where T : IStateStore
    {
        private IDictionary<string, string> logConfig = new Dictionary<string, string>();
        
        /// <summary>
        /// Name of the state store
        /// </summary>
        private readonly string name;
        
        /// <summary>
        /// Key serdes
        /// </summary>
        protected readonly ISerDes<K> keySerdes;
        
        /// <summary>
        /// Value serdes
        /// </summary>
        protected readonly ISerDes<V> valueSerdes;

        private bool enableCaching = false;
        private bool enableLogging = true;

        /// <summary>
        /// Is window store or not
        /// </summary>
        public abstract bool IsWindowStore { get; }
        
        /// <summary>
        /// retention (in milliseconds) of the state store 
        /// </summary>
        public abstract long RetentionMs { get; }
        
        /// <summary>
        /// Name of the state store
        /// </summary>
        public string Name => name;
        
        /// <summary>
        /// Configuration of the changelog topic
        /// </summary>
        public IDictionary<string, string> LogConfig => logConfig;
        
        /// <summary>
        /// Logging enabled or not
        /// </summary>
        public bool LoggingEnabled => enableLogging;

        /// <summary>
        /// Caching enabled or not
        /// </summary>
        public bool CachingEnabled => enableCaching;

        /// <summary>
        /// Cache size of the storage
        /// </summary>
        public CacheSize CacheSize { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="keySerde"></param>
        /// <param name="valueSerde"></param>
        protected AbstractStoreBuilder(String name, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            this.name = name;
            keySerdes = keySerde;
            valueSerdes = valueSerde;
        }

        /// <summary>
        /// Activate caching
        /// </summary>
        /// <returns></returns>
        public IStoreBuilder<T> WithCachingEnabled(CacheSize cacheSize = null)
        {
            enableCaching = true;
            CacheSize = cacheSize;
            return this;
        }

        /// <summary>
        /// Disable caching
        /// </summary>
        /// <returns></returns>
        public IStoreBuilder<T> WithCachingDisabled()
        {
            enableCaching = false;
            return this;
        }

        /// <summary>
        /// Activate logging
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        public IStoreBuilder<T> WithLoggingEnabled(IDictionary<String, String> config)
        {
            enableLogging = true;
            logConfig = config;
            return this;
        }

        /// <summary>
        /// Disable logging
        /// </summary>
        /// <returns></returns>
        public IStoreBuilder<T> WithLoggingDisabled()
        {
            enableLogging = false;
            logConfig.Clear();
            return this;
        }

        /// <summary>
        /// Build the state store
        /// </summary>
        /// <returns></returns>
        public abstract T Build();

        IStateStore IStoreBuilder.Build() => Build();
    }
}
