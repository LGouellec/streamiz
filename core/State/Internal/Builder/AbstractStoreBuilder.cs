using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State.Internal.Builder
{
    internal abstract class AbstractStoreBuilder<K, V, T> : StoreBuilder<T>
        where T : StateStore
    {
        private IDictionary<string, string> logConfig = new Dictionary<string, string>();
        protected readonly string name;
        protected readonly ISerDes<K> keySerdes;
        protected readonly ISerDes<V> valueSerdes;
        private DateTime time;
        bool enableCaching;
        bool enableLogging = true;

        public string Name => name;
        public IDictionary<string, string> LogConfig => logConfig;
        public bool LoggingEnabled => enableLogging;

        public AbstractStoreBuilder(String name, ISerDes<K> keySerde, ISerDes<V> valueSerde, DateTime time)
        {
            this.name = name;
            this.keySerdes = keySerde;
            this.valueSerdes = valueSerde;
            this.time = time;
        }


        public StoreBuilder<T> withCachingEnabled()
        {
            enableCaching = true;
            return this;
        }


        public StoreBuilder<T> withCachingDisabled()
        {
            enableCaching = false;
            return this;
        }


        public StoreBuilder<T> withLoggingEnabled(IDictionary<String, String> config)
        {
            enableLogging = true;
            logConfig = config;
            return this;
        }


        public StoreBuilder<T> withLoggingDisabled()
        {
            enableLogging = false;
            logConfig.Clear();
            return this;
        }

        public abstract T build();

        object StoreBuilder.build() => this.build();
    }
}
