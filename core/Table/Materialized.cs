using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using kafka_stream_core.State;
using kafka_stream_core.Stream.Internal;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Table
{
    public class Materialized<K, V, S>
        where S : StateStore
    {
        private bool queriable = false;
        protected StoreSupplier<S> storeSupplier;
        protected string storeName;
        protected ISerDes<V> valueSerde;
        protected ISerDes<K> keySerde;
        protected bool loggingEnabled = true;
        protected bool cachingEnabled = true;
        protected IDictionary<string, string> topicConfig = new Dictionary<string, string>();
        protected TimeSpan retention;

        #region Ctor

        private Materialized(StoreSupplier<S> storeSupplier)
        {
            this.storeSupplier = storeSupplier;
        }

        private Materialized(string storeName)
        {
            this.storeName = storeName;
        }

        protected Materialized(Materialized<K, V, S> materialized)
        {
            this.storeSupplier = materialized.storeSupplier;
            this.storeName = materialized.storeName;
            this.keySerde = materialized.keySerde;
            this.valueSerde = materialized.valueSerde;
            this.loggingEnabled = materialized.loggingEnabled;
            this.cachingEnabled = materialized.cachingEnabled;
            this.topicConfig = materialized.topicConfig;
            this.retention = materialized.retention;
        }

        #endregion

        #region Static

        public static Materialized<K, V, S> @as<K, V, S>(string storeName) where S : StateStore
        {
            return new Materialized<K, V, S>(storeName);
        }

        public static Materialized<K, V, WindowStore<byte[], byte[]>> @as<K, V>(WindowBytesStoreSupplier supplier)
        {
            return new Materialized<K, V, WindowStore<byte[], byte[]>>(supplier);
        }

        public static Materialized<K, V, SessionStore<byte[], byte[]>> @as<K, V>(SessionBytesStoreSupplier supplier)
        {
            return new Materialized<K, V, SessionStore<byte[], byte[]>>(supplier);
        }

        public static Materialized<K, V, KeyValueStore<byte[], byte[]>> @as<K, V>(KeyValueBytesStoreSupplier supplier)
        {
            return new Materialized<K, V, KeyValueStore<byte[], byte[]>>(supplier);
        }

        public static Materialized<K, V, S> with<K, V, S>(ISerDes<K> keySerde, ISerDes<V> valueSerde)
            where S : StateStore
        {
            return new Materialized<K, V, S>(string.Empty).withKeySerde(keySerde).withValueSerde(valueSerde);
        }

        #endregion

        #region Property

        public string StoreName => storeSupplier != null ? storeSupplier.Name : storeName;

        #endregion

        public Materialized<K, V, S> withValueSerde(ISerDes<V> valueSerde)
        {
            this.valueSerde = valueSerde;
            return this;
        }

        public Materialized<K, V, S> withKeySerde(ISerDes<K> keySerde)
        {
            this.keySerde = keySerde;
            return this;
        }

        public Materialized<K, V, S> withLoggingEnabled(IDictionary<string, string> config)
        {
            loggingEnabled = true;
            this.topicConfig = config;
            return this;
        }

        public Materialized<K, V, S> withLoggingDisabled()
        {
            loggingEnabled = false;
            this.topicConfig.Clear();
            return this;
        }

        public Materialized<K, V, S> withCachingEnabled()
        {
            cachingEnabled = true;
            return this;
        }

        public Materialized<K, V, S> withCachingDisabled()
        {
            cachingEnabled = false;
            return this;
        }

        public Materialized<K, V, S> withRetention(TimeSpan retention)
        {
            double retenationMs = retention.TotalMilliseconds;

            if (retenationMs < 0)
            {
                throw new ArgumentException("Retention must not be negative.");
            }

            this.retention = retention;
            return this;
        }
    
        internal Materialized<K, V, S> useProvider(InternalNameProvider provider, string generatedStorePrefix)
        {
            queriable = StoreName != null;
            if (!queriable && provider != null)
            {
                storeName = provider.newStoreName(generatedStorePrefix);
            }

            return this;
        }

    }
}
