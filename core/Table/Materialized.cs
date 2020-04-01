using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using kafka_stream_core.State;
using kafka_stream_core.State.InMemory;
using kafka_stream_core.State.Supplier;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Table
{
    public class Materialized<K, V, S>
        where S : IStateStore
    {
        private bool queriable = false;
        protected string storeName;
        protected bool loggingEnabled = true;
        protected bool cachingEnabled = true;
        protected TimeSpan retention;

        #region Ctor

        protected Materialized(string storeName, StoreSupplier<S> storeSupplier)
        {
            this.storeName = storeName;
            this.StoreSupplier = storeSupplier;
        }

        protected Materialized(StoreSupplier<S> storeSupplier)
        {
            this.StoreSupplier = storeSupplier;
        }

        protected Materialized(string storeName)
        {
            this.storeName = storeName;
        }

        protected Materialized(Materialized<K, V, S> materialized)
        {
            this.StoreSupplier = materialized.StoreSupplier;
            this.storeName = materialized.storeName;
            this.KeySerdes = materialized.KeySerdes;
            this.ValueSerdes = materialized.ValueSerdes;
            this.loggingEnabled = materialized.loggingEnabled;
            this.cachingEnabled = materialized.cachingEnabled;
            this.TopicConfig = materialized.TopicConfig;
            this.retention = materialized.retention;
        }

        #endregion

        #region Static

        public static Materialized<K, V, S> Create() => new Materialized<K, V, S>(string.Empty);

        public static Materialized<K, V, S> Create<KS, VS>() 
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new() 
            => Create<KS, VS>(string.Empty);

        public static Materialized<K, V, S> Create<KS, VS>(string storeName) 
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new Materialized<K, V, S>(storeName);
            m.KeySerdes = new KS();
            m.ValueSerdes = new VS();
            return m;
        }

        public static Materialized<K, V, WindowStore<Bytes, byte[]>> Create<KS, VS>(WindowBytesStoreSupplier supplier)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new Materialized<K, V, WindowStore<Bytes, byte[]>>(supplier);
            m.KeySerdes = new KS();
            m.ValueSerdes = new VS();
            return m;
        }

        public static Materialized<K, V, SessionStore<Bytes, byte[]>> Create<KS, VS>(SessionBytesStoreSupplier supplier)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new Materialized<K, V, SessionStore<Bytes, byte[]>>(supplier);
            m.KeySerdes = new KS();
            m.ValueSerdes = new VS();
            return m;
        }

        public static Materialized<K, V, KeyValueStore<Bytes, byte[]>> Create<KS, VS>(KeyValueBytesStoreSupplier supplier)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new Materialized<K, V, KeyValueStore<Bytes, byte[]>>(supplier);
            m.KeySerdes = new KS();
            m.ValueSerdes = new VS();
            return m;
        }

        #endregion

        #region Property

        public IDictionary<string, string> TopicConfig { get; protected set; }

        public bool LoggingEnabled { get; protected set; }

        public bool CachingEnabled { get; protected set; }

        public StoreSupplier<S> StoreSupplier { get; protected set; }

        public ISerDes<K> KeySerdes { get; protected set; }

        public ISerDes<V> ValueSerdes { get; protected set; }

        public string StoreName => StoreSupplier != null ? StoreSupplier.Name : storeName;

        public string QueryableStoreName => queriable ? StoreName : null;

        #endregion

        #region Methods

        public Materialized<K, V, S> WithLoggingEnabled(IDictionary<string, string> config)
        {
            loggingEnabled = true;
            this.TopicConfig = config;
            return this;
        }

        public Materialized<K, V, S> WithLoggingDisabled()
        {
            loggingEnabled = false;
            this.TopicConfig.Clear();
            return this;
        }

        public Materialized<K, V, S> WithCachingEnabled()
        {
            cachingEnabled = true;
            return this;
        }

        public Materialized<K, V, S> WithCachingDisabled()
        {
            cachingEnabled = false;
            return this;
        }

        public Materialized<K, V, S> WithRetention(TimeSpan retention)
        {
            double retentionMs = retention.TotalMilliseconds;

            if (retentionMs < 0)
            {
                throw new ArgumentException("Retention must not be negative.");
            }

            this.retention = retention;
            return this;
        }
    
        internal Materialized<K, V, S> UseProvider(INameProvider provider, string generatedStorePrefix)
        {
            queriable = !string.IsNullOrEmpty(StoreName);
            if (!queriable && provider != null)
            {
                storeName = provider.NewStoreName(generatedStorePrefix);
            }

            return this;
        }

        internal Materialized<K, V, S> InitConsumed(Consumed<K, V> consumed)
        {
            if (this.KeySerdes == null)
                this.KeySerdes = consumed.KeySerdes;
            if (this.ValueSerdes == null)
                this.ValueSerdes = consumed.ValueSerdes;

            return this;
        }
        
        #endregion
    }

    #region Child Materialized

    public class InMemory<K, V> : Materialized<K, V, KeyValueStore<Bytes, byte[]>>
    {
        protected InMemory(string name, StoreSupplier<KeyValueStore<Bytes, byte[]>> supplier) 
            : base(name, supplier)
        {

        }

        public static InMemory<K, V> @As(string storeName) => new InMemory<K, V>(storeName, new InMemoryKeyValueBytesStoreSupplier(storeName));

        public static InMemory<K, V> @As<KS, VS>(string storeName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new InMemory<K,V>(storeName, new InMemoryKeyValueBytesStoreSupplier(storeName));
            m.KeySerdes = new KS();
            m.ValueSerdes = new VS();
            return m;
        }
    }

    #endregion
}
