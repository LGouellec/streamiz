using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Table
{
    /// <summary>
    /// Used to describe how a <see cref="IStateStore"/> should be materialized.
    /// You can either provide a custom <see cref="IStateStore"/> backend through one of the provided methods accepting a supplier
    /// or use the default RocksDB backends by providing just a store name.
    /// For example, you can read a topic as <see cref="IKTable{K, V}"/> and force a state store materialization
    /// <example>
    /// <code>
    /// var builder = new StreamBuilder();
    /// builder.Table("topic",
    ///         Materialized&lt;byte[], byte[], IKeyValueStore&lt;Bytes, byte[]&gt;&gt;.Create("test-store"));
    /// </code>
    /// </example>
    /// </summary>
    /// <typeparam name="K">type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    /// <typeparam name="S">type of state store (note: state stores always have key/value types &lt;<see cref="Bytes"/>,byte[]&gt;)</typeparam>
    public class Materialized<K, V, S>
        where S : IStateStore
    {
        private bool queriable = false;

        /// <summary>
        /// Name of state store
        /// </summary>
        protected string storeName;

        /// <summary>
        /// Retention time
        /// </summary>
        private TimeSpan retention;

        #region Ctor

        /// <summary>
        /// Protected constructor with state store name and supplier
        /// </summary>
        /// <param name="storeName">State store name for query it</param>
        /// <param name="storeSupplier">Supplier use to build the state store</param>
        protected Materialized(string storeName, IStoreSupplier<S> storeSupplier)
        {
            this.storeName = storeName;
            StoreSupplier = storeSupplier;
            retention = TimeSpan.FromDays(1);
        }

        /// <summary>
        /// Protected constructor with store supplier
        /// </summary>
        /// <param name="storeSupplier">Supplier use to build the state store</param>
        private Materialized(IStoreSupplier<S> storeSupplier)
            : this(null, storeSupplier)
        {
        }

        /// <summary>
        /// Protected constructor with state store name
        /// </summary>
        /// <param name="storeName">State store name for query it</param>
        private Materialized(string storeName)
            : this(storeName, null)
        {
        }
        
        #endregion

        #region Static

        /// <summary>
        /// Materialize a <see cref="IStateStore"/> with the given name.
        /// </summary>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given storeName</returns>
        public static Materialized<K, V, S> Create(string storeName) => new(storeName);

        /// <summary>
        /// Materialize a <see cref="IStateStore"/>. The store name will be a empty string (so, it's not queryable).
        /// </summary>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance</returns>
        public static Materialized<K, V, S> Create() => new(string.Empty);

        /// <summary>
        /// Materialize a <see cref="IWindowStore{K, V}"/> using the provided <see cref="IWindowBytesStoreSupplier"/>
        /// Important: Custom subclasses are allowed here, but they should respect the retention contract:
        /// Window stores are required to retain windows at least as long as (window size + window grace period).
        /// </summary>
        /// <param name="supplier">the <see cref="IWindowBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, IWindowStore<Bytes, byte[]>> Create(IWindowBytesStoreSupplier supplier)
        {
            var m = new Materialized<K, V, IWindowStore<Bytes, byte[]>>(supplier);
            return m;
        }

        // /// <summary>
        // /// Materialize a <see cref="ISessionStore{K, AGG}"/> using the provided <see cref="ISessionBytesStoreSupplier"/>
        // /// Important: Custom subclasses are allowed here, but they should respect the retention contract:
        // /// Session stores are required to retain windows at least as long as (session inactivity gap + session grace period).
        // /// </summary>
        // /// <param name="supplier">the <see cref="ISessionBytesStoreSupplier"/> used to materialize the store</param>
        // /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        // public static Materialized<K, V, ISessionStore<Bytes, byte[]>> Create(ISessionBytesStoreSupplier supplier)
        // {
        //     var m = new Materialized<K, V, ISessionStore<Bytes, byte[]>>(supplier);
        //     return m;
        // }

        /// <summary>
        /// Materialize a <see cref="IKeyValueStore{K, V}"/> using the provided <see cref="IKeyValueBytesStoreSupplier"/>
        /// </summary>
        /// <param name="supplier">the <see cref="IKeyValueBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> Create(IKeyValueBytesStoreSupplier supplier)
        {
            var m = new Materialized<K, V, IKeyValueStore<Bytes, byte[]>>(supplier);
            return m;
        }

        /// <summary>
        /// Materialize a <see cref="IStateStore"/>. The store name will be a empty string (so, it's not queryable).
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance</returns>
        public static Materialized<K, V, S> Create<KS, VS>()
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Create<KS, VS>(string.Empty);

        /// <summary>
        /// Materialize a <see cref="IStateStore"/> with the given name.
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given storeName</returns>
        public static Materialized<K, V, S> Create<KS, VS>(string storeName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return Create(storeName).WithKeySerdes(new KS()).WithValueSerdes(new VS());
        }

        /// <summary>
        /// Materialize a <see cref="IWindowStore{K, V}"/> using the provided <see cref="IWindowBytesStoreSupplier"/>
        /// Important: Custom subclasses are allowed here, but they should respect the retention contract:
        /// Window stores are required to retain windows at least as long as (window size + window grace period).
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="supplier">the <see cref="IWindowBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, IWindowStore<Bytes, byte[]>> Create<KS, VS>(IWindowBytesStoreSupplier supplier)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return Create(supplier).WithKeySerdes(new KS()).WithValueSerdes(new VS());
        }

        // /// <summary>
        // /// Materialize a <see cref="ISessionStore{K, AGG}"/> using the provided <see cref="ISessionBytesStoreSupplier"/>
        // /// Important: Custom subclasses are allowed here, but they should respect the retention contract:
        // /// Session stores are required to retain windows at least as long as (session inactivity gap + session grace period).
        // /// </summary>
        // /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        // /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        // /// <param name="supplier">the <see cref="ISessionBytesStoreSupplier"/> used to materialize the store</param>
        // /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        // public static Materialized<K, V, ISessionStore<Bytes, byte[]>> Create<KS, VS>(ISessionBytesStoreSupplier supplier)
        //     where KS : ISerDes<K>, new()
        //     where VS : ISerDes<V>, new()
        // {
        //     return Create(supplier).WithKeySerdes(new KS()).WithValueSerdes(new VS());
        // }

        /// <summary>
        /// Materialize a <see cref="IKeyValueStore{K, V}"/> using the provided <see cref="IKeyValueBytesStoreSupplier"/>
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="supplier">the <see cref="IKeyValueBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> Create<KS, VS>(IKeyValueBytesStoreSupplier supplier)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return Create(supplier).WithKeySerdes(new KS()).WithValueSerdes(new VS());
        }

        #endregion

        #region Property

        /// <summary>
        /// Is state store is not materialized. Default : false, so it's materialized.
        /// </summary>
        public bool NoMaterialized { get; set; } = false;

        /// <summary>
        /// Topic configuration
        /// </summary>
        public IDictionary<string, string> TopicConfig { get; protected set; }

        /// <summary>
        /// Is logging enabled (default: false), Warning : will be true in next release.
        /// </summary>
        public bool LoggingEnabled { get; protected set; } = true;

        /// <summary>
        /// Is caching enabled. Not use for moment.
        /// </summary>
        public bool CachingEnabled { get; protected set; }

        /// <summary>
        /// Store suppplier use to build the state store
        /// </summary>
        public IStoreSupplier<S> StoreSupplier { get; protected set; }

        /// <summary>
        /// Key serdes
        /// </summary>
        public ISerDes<K> KeySerdes { get; protected set; }

        /// <summary>
        /// Value serdes
        /// </summary>
        public ISerDes<V> ValueSerdes { get; protected set; }

        /// <summary>
        /// Name of the state store. If supplier is not null, return supplier name else name of store.
        /// </summary>
        public string StoreName => StoreSupplier != null ? StoreSupplier.Name : storeName;

        /// <summary>
        /// Return <see cref="StoreName"/> if the <see cref="Materialized{K, V, S}"/> is queryable.
        /// </summary>
        public string QueryableStoreName => queriable ? StoreName : null;

        /// <summary>
        /// Retention configuration (default : one day)
        /// </summary>
        public TimeSpan Retention => retention;

        #endregion

        #region Methods
        
        /// <summary>
        /// Set the materialized name
        /// </summary>
        /// <param name="storeName">Name of the state store</param>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithName(string storeName)
        {
            this.storeName = storeName;
            return this;
        }
                
        /// <summary>
        /// Enable logging with topic configuration for this <see cref="Materialized{K, V, S}"/>
        /// </summary>
        /// <param name="config">Topic configuration dictionnary, can be null</param>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithLoggingEnabled(IDictionary<string, string> config = null)
        {
            LoggingEnabled = true;
            TopicConfig = config ?? new Dictionary<string, string>();
            return this;
        }

        /// <summary>
        /// Disable logging for this <see cref="Materialized{K, V, S}"/>
        /// </summary>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithLoggingDisabled()
        {
            LoggingEnabled = false;
            TopicConfig?.Clear();
            return this;
        }

        /// <summary>
        /// Enable caching for this <see cref="Materialized{K, V, S}"/>
        /// </summary>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithCachingEnabled()
        {
            CachingEnabled = true;
            return this;
        }

        /// <summary>
        /// Disable caching for this <see cref="Materialized{K, V, S}"/>
        /// </summary>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithCachingDisabled()
        {
            CachingEnabled = false;
            return this;
        }

        /// <summary>
        /// Configure retention period for window and session stores. Ignored for key/value stores.
        /// Note that the retention period must be at least long enough to contain the windowed data's entire life cycle,
        /// from window-start through window-end, and for the entire grace period.
        /// </summary>
        /// <param name="retention">Retention time</param>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithRetention(TimeSpan retention)
        {
            double retentionMs = retention.TotalMilliseconds;

            if (retentionMs < 0)
            {
                throw new ArgumentException("Retention must not be negative.");
            }

            this.retention = retention;
            if (StoreSupplier is IWindowBytesStoreSupplier windowBytesStoreSupplier)
                windowBytesStoreSupplier.Retention = (long) retention.TotalMilliseconds;
            
            return this;
        }

        /// <summary>
        /// Configure key and value serdes
        /// </summary>
        /// <typeparam name="KS">Key serdes type</typeparam>
        /// <typeparam name="VS">Value serdes type</typeparam>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> With<KS, VS>()
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            KeySerdes = new KS();
            ValueSerdes = new VS();
            return this;
        }

        /// <summary>
        /// Configure key and value serdes
        /// </summary>
        /// <param name="keySerdes">Key serdes</param>
        /// <param name="valueSerdes">Value serdes</param>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> With(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            return this;
        }

        /// <summary>
        /// Configure key serdes
        /// </summary>
        /// <param name="keySerdes">Key serdes</param>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithKeySerdes(ISerDes<K> keySerdes)
        {
            KeySerdes = keySerdes;
            return this;
        }

        /// <summary>
        /// Configure value serdes
        /// </summary>
        /// <param name="valueSerdes">Value serdes</param>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithValueSerdes(ISerDes<V> valueSerdes)
        {
            ValueSerdes = valueSerdes;
            return this;
        }

        /// <summary>
        /// Configure key serdes
        /// </summary>
        /// <typeparam name="KRS">New key serdes type</typeparam>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithKeySerdes<KRS>()
            where KRS : ISerDes<K>, new()
        {
            KeySerdes = new KRS();
            return this;
        }

        /// <summary>
        /// Configure value serdes
        /// </summary>
        /// <typeparam name="VRS">New value serdes type</typeparam>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithValueSerdes<VRS>()
            where VRS : ISerDes<V>, new()
        {
            ValueSerdes = new VRS();
            return this;
        }

        #region Internal

        internal Materialized<K, V, S> UseProvider(INameProvider provider, string generatedStorePrefix)
        {
            queriable = !string.IsNullOrEmpty(StoreName);
            if (!queriable && provider != null && !NoMaterialized)
            {
                storeName = provider.NewStoreName(generatedStorePrefix);
                if (StoreSupplier is {Name: null})
                    StoreSupplier.Name = storeName;
                queriable = true;
            }

            return this;
        }

        internal Materialized<K, V, S> InitConsumed(ConsumedInternal<K, V> consumed)
        {
            if (KeySerdes == null)
                KeySerdes = consumed.KeySerdes;
            if (ValueSerdes == null)
                ValueSerdes = consumed.ValueSerdes;

            return this;
        }

        #endregion

        #endregion
    }

    #region Helper Materialized

    /// <summary>
    /// <see cref="InMemory"/> is a helper class to build a <see cref="Materialized{K, V, S}"/> for an in-memory store.
    /// </summary>
    public static class InMemory
    {
        /// <summary>
        /// Materialize a <see cref="InMemoryKeyValueStore"/> with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>(string storeName = null)
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create(new InMemoryKeyValueBytesStoreSupplier(storeName))
                    .WithName(storeName);
            return materialized;
        }
        
        /// <summary>
        /// Materialize a <see cref="InMemoryKeyValueStore"/> with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V, KS, VS>(string storeName = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create<KS, VS>(new InMemoryKeyValueBytesStoreSupplier(storeName))
                    .WithName(storeName);
            return materialized;
        }
    }

    /// <summary>
    /// <see cref="InMemoryWindows"/> is a helper class to build a <see cref="Materialized{K, V, S}"/> for an in-memory window store. 
    /// </summary>
    public static class InMemoryWindows
    {
        /// <summary>
        /// Materialize a <see cref="InMemoryWindowStore"/> with the given name.
        /// </summary>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <param name="windowSize">the windows size aggregation</param>
        /// <returns>a new materialized instance with the given storeName and windows size</returns>
        public static Materialized<K, V, IWindowStore<Bytes, byte[]>> @As<K, V>(string storeName = null, TimeSpan? windowSize = null)
        {
            Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized =
                Materialized<K, V, IWindowStore<Bytes, byte[]>>
                    .Create(new InMemoryWindowStoreSupplier(storeName, TimeSpan.FromDays(1),
                        windowSize.HasValue ? (long) windowSize.Value.TotalMilliseconds : (long?) null))
                    .WithName(storeName);
            
            return materialized;
        }

        /// <summary>
        /// Materialize a <see cref="InMemoryWindowStore"/> with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <param name="windowSize">the windows size aggregation</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IWindowStore<Bytes, byte[]>> @As<K, V, KS, VS>(string storeName = null, TimeSpan? windowSize = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized =
                Materialized<K, V, IWindowStore<Bytes, byte[]>>
                    .Create<KS, VS>(new InMemoryWindowStoreSupplier(storeName, TimeSpan.FromDays(1),
                        windowSize.HasValue ? (long) windowSize.Value.TotalMilliseconds : (long?) null))
                    .WithName(storeName);
            
            return materialized;
        }
    }

    /// <summary>
    /// <see cref="RocksDb"/> is a helper class to build a <see cref="Materialized{K, V, S}"/> for a rocksdb store. 
    /// </summary>
    public static class RocksDb
    {
        /// <summary>
        /// Materialize a <see cref="RocksDbKeyValueStore"/> with the given name.
        /// </summary>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>(string storeName = null)
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create(new RocksDbKeyValueBytesStoreSupplier(storeName))
                    .WithName(storeName);
            return materialized;
        }
        
        /// <summary>
        /// Materialize a <see cref="RocksDbKeyValueStore"/> with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V, KS, VS>(string storeName = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create<KS, VS>(new RocksDbKeyValueBytesStoreSupplier(storeName))
                    .WithName(storeName);
            return materialized;
        }
    }

    /// <summary>
    /// <see cref="RocksDbWindows"/> is a helper class to build a <see cref="Materialized{K, V, S}"/> for a rocksdb window store. 
    /// It's a class helper for materialize <see cref="IKTable{K, V}"/> with an <see cref="RocksDbWindowBytesStoreSupplier"/>
    /// </summary>
    public static class RocksDbWindows
    {
        /// <summary>
        /// Materialize a <see cref="RocksDbWindowStore"/> with the given name.
        /// </summary>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <param name="segmentInterval"></param>
        /// <param name="windowSize">the windows size aggregation</param>
        /// <returns>a new materialized instance with the given storeName and windows size</returns>
        public static Materialized<K, V, IWindowStore<Bytes, byte[]>> @As<K, V>(string storeName = null,
            TimeSpan? segmentInterval = null, TimeSpan? windowSize = null)
        {
            Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized =
                Materialized<K, V, IWindowStore<Bytes, byte[]>>
                    .Create(new RocksDbWindowBytesStoreSupplier(
                        storeName,
                        TimeSpan.FromDays(1),
                        segmentInterval.HasValue ? (long)segmentInterval.Value.TotalMilliseconds : 60 * 1000 * 60,
                        windowSize.HasValue ? (long)windowSize.Value.TotalMilliseconds : (long?)null))
                    .WithName(storeName);
            
            return materialized;
        }

        /// <summary>
        /// Materialize a <see cref="RocksDbWindowStore"/> with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <param name="segmentInterval"></param>
        /// <param name="windowSize">the windows size aggregation</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IWindowStore<Bytes, byte[]>> @As<K, V, KS, VS>(string storeName = null, TimeSpan? segmentInterval = null, TimeSpan? windowSize = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized =
                Materialized<K, V, IWindowStore<Bytes, byte[]>>
                    .Create<KS, VS>(new RocksDbWindowBytesStoreSupplier(
                        storeName,
                        TimeSpan.FromDays(1),
                        segmentInterval.HasValue ? (long)segmentInterval.Value.TotalMilliseconds : 60 * 1000 * 60,
                        windowSize.HasValue ? (long)windowSize.Value.TotalMilliseconds : (long?)null))
                    .WithName(storeName);
            
            return materialized;
        }
    }

    #endregion
}