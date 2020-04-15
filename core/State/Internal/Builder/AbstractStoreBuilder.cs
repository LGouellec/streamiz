using Kafka.Streams.Net.Processors;
using Kafka.Streams.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State.Internal.Builder
{
    internal abstract class AbstractStoreBuilder<K, V, T> : StoreBuilder<T>
        where T : IStateStore
    {
        private IDictionary<string, string> logConfig = new Dictionary<string, string>();
        protected readonly string name;
        protected readonly ISerDes<K> keySerdes;
        protected readonly ISerDes<V> valueSerdes;
        bool enableCaching;
        bool enableLogging = true;

        public string Name => name;
        public IDictionary<string, string> LogConfig => logConfig;
        public bool LoggingEnabled => enableLogging;

        public AbstractStoreBuilder(String name, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            this.name = name;
            this.keySerdes = keySerde;
            this.valueSerdes = valueSerde;
        }


        public StoreBuilder<T> WithCachingEnabled()
        {
            enableCaching = true;
            return this;
        }


        public StoreBuilder<T> WithCachingDisabled()
        {
            enableCaching = false;
            return this;
        }


        public StoreBuilder<T> WithLoggingEnabled(IDictionary<String, String> config)
        {
            enableLogging = true;
            logConfig = config;
            return this;
        }


        public StoreBuilder<T> WithLoggingDisabled()
        {
            enableLogging = false;
            logConfig.Clear();
            return this;
        }

        public abstract T Build();

        object StoreBuilder.build() => this.Build();
    }
}
