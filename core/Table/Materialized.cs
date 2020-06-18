using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
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
        protected TimeSpan retention;

        #region Ctor

        /// <summary>
        /// Protected constructor with state store name and supplier
        /// </summary>
        /// <param name="storeName">State store name for query it</param>
        /// <param name="storeSupplier">Supplier use to build the state store</param>
        protected Materialized(string storeName, StoreSupplier<S> storeSupplier)
        {
            this.storeName = storeName;
            StoreSupplier = storeSupplier;
            retention = TimeSpan.FromDays(1);
        }

        /// <summary>
        /// Protected constructor with store supplier
        /// </summary>
        /// <param name="storeSupplier">Supplier use to build the state store</param>
        protected Materialized(StoreSupplier<S> storeSupplier)
            : this(null, storeSupplier)
        {
        }

        /// <summary>
        /// Protected constructor with state store name
        /// </summary>
        /// <param name="storeName">State store name for query it</param>
        protected Materialized(string storeName)
            : this(storeName, null)
        {
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        /// <param name="materialized">Materialized to copy</param>
        protected Materialized(Materialized<K, V, S> materialized)
             : this(materialized.StoreName, materialized.StoreSupplier)
        {
            StoreSupplier = materialized.StoreSupplier;
            storeName = materialized.storeName;
            KeySerdes = materialized.KeySerdes;
            ValueSerdes = materialized.ValueSerdes;
            LoggingEnabled = materialized.LoggingEnabled;
            CachingEnabled = materialized.CachingEnabled;
            TopicConfig = materialized.TopicConfig;
            retention = materialized.retention;
        }

        #endregion

        #region Static

        /// <summary>
        /// Materialize a <see cref="IStateStore"/> with the given name.
        /// </summary>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given storeName</returns>
        public static Materialized<K, V, S> Create(string storeName) => new Materialized<K, V, S>(storeName);

        /// <summary>
        /// Materialize a <see cref="IStateStore"/>. The store name will be a empty string (so, it's not queryable).
        /// </summary>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance</returns>
        public static Materialized<K, V, S> Create() => new Materialized<K, V, S>(string.Empty);

        /// <summary>
        /// Materialize a <see cref="WindowStore{K, V}"/> using the provided <see cref="WindowBytesStoreSupplier"/>
        /// Important: Custom subclasses are allowed here, but they should respect the retention contract:
        /// Window stores are required to retain windows at least as long as (window size + window grace period).
        /// </summary>
        /// <param name="supplier">the <see cref="WindowBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, WindowStore<Bytes, byte[]>> Create(WindowBytesStoreSupplier supplier)
        {
            var m = new Materialized<K, V, WindowStore<Bytes, byte[]>>(supplier);
            return m;
        }

        /// <summary>
        /// Materialize a <see cref="SessionStore{K, AGG}"/> using the provided <see cref="SessionBytesStoreSupplier"/>
        /// Important: Custom subclasses are allowed here, but they should respect the retention contract:
        /// Session stores are required to retain windows at least as long as (session inactivity gap + session grace period).
        /// </summary>
        /// <param name="supplier">the <see cref="SessionBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, SessionStore<Bytes, byte[]>> Create(SessionBytesStoreSupplier supplier)
        {
            var m = new Materialized<K, V, SessionStore<Bytes, byte[]>>(supplier);
            return m;
        }

        /// <summary>
        /// Materialize a <see cref="IKeyValueStore{K, V}"/> using the provided <see cref="KeyValueBytesStoreSupplier"/>
        /// </summary>
        /// <param name="supplier">the <see cref="KeyValueBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> Create(KeyValueBytesStoreSupplier supplier)
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
            var m = new Materialized<K, V, S>(storeName)
            {
                KeySerdes = new KS(),
                ValueSerdes = new VS()
            };
            return m;
        }

        /// <summary>
        /// Materialize a <see cref="WindowStore{K, V}"/> using the provided <see cref="WindowBytesStoreSupplier"/>
        /// Important: Custom subclasses are allowed here, but they should respect the retention contract:
        /// Window stores are required to retain windows at least as long as (window size + window grace period).
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="supplier">the <see cref="WindowBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, WindowStore<Bytes, byte[]>> Create<KS, VS>(WindowBytesStoreSupplier supplier)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new Materialized<K, V, WindowStore<Bytes, byte[]>>(supplier)
            {
                KeySerdes = new KS(),
                ValueSerdes = new VS()
            };
            return m;
        }

        /// <summary>
        /// Materialize a <see cref="SessionStore{K, AGG}"/> using the provided <see cref="SessionBytesStoreSupplier"/>
        /// Important: Custom subclasses are allowed here, but they should respect the retention contract:
        /// Session stores are required to retain windows at least as long as (session inactivity gap + session grace period).
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="supplier">the <see cref="SessionBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, SessionStore<Bytes, byte[]>> Create<KS, VS>(SessionBytesStoreSupplier supplier)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new Materialized<K, V, SessionStore<Bytes, byte[]>>(supplier)
            {
                KeySerdes = new KS(),
                ValueSerdes = new VS()
            };
            return m;
        }

        /// <summary>
        /// Materialize a <see cref="IKeyValueStore{K, V}"/> using the provided <see cref="KeyValueBytesStoreSupplier"/>
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="supplier">the <see cref="KeyValueBytesStoreSupplier"/> used to materialize the store</param>
        /// <returns>a new <see cref="Materialized{K, V, S}"/> instance with the given supplier</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> Create<KS, VS>(KeyValueBytesStoreSupplier supplier)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new Materialized<K, V, IKeyValueStore<Bytes, byte[]>>(supplier)
            {
                KeySerdes = new KS(),
                ValueSerdes = new VS()
            };
            return m;
        }

        #endregion

        #region Property

        /// <summary>
        /// Topic configuration
        /// </summary>
        public IDictionary<string, string> TopicConfig { get; protected set; }

        /// <summary>
        /// Is logging enabled
        /// </summary>
        public bool LoggingEnabled { get; protected set; }

        /// <summary>
        /// Is caching enabled
        /// </summary>
        public bool CachingEnabled { get; protected set; }

        /// <summary>
        /// Store suppplier use to build the state store
        /// </summary>
        public StoreSupplier<S> StoreSupplier { get; protected set; }

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
        /// Enable logging with topic configuration for this <see cref="Materialized{K, V, S}"/>
        /// </summary>
        /// <param name="config">Topic configuration dictionnary</param>
        /// <returns>Itself</returns>
        public Materialized<K, V, S> WithLoggingEnabled(IDictionary<string, string> config)
        {
            LoggingEnabled = true;
            TopicConfig = config;
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
            if (!queriable && provider != null)
            {
                storeName = provider.NewStoreName(generatedStorePrefix);
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

    #region Child Materialized

    /// <summary>
    /// <see cref="InMemory{K, V}"/> is a child class of <see cref="Materialized{K, V, S}"/>. 
    /// It's a class helper for materialize <see cref="IKTable{K, V}"/> with an <see cref="InMemoryKeyValueBytesStoreSupplier"/>
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">type of value</typeparam>
    public class InMemory<K, V> : Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
    {
        /// <summary>
        /// Protected constructor with state store name and supplier
        /// </summary>
        /// <param name="name">State store name for query it</param>
        /// <param name="supplier">Supplier use to build the state store</param>
        protected InMemory(string name, StoreSupplier<IKeyValueStore<Bytes, byte[]>> supplier)
            : base(name, supplier)
        {

        }

        /// <summary>
        /// Materialize a <see cref="InMemoryKeyValueStore"/> with the given name.
        /// </summary>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new <see cref="InMemory{K, V}"/> instance with the given storeName</returns>
        public static InMemory<K, V> @As(string storeName)
            => new InMemory<K, V>(storeName, new InMemoryKeyValueBytesStoreSupplier(storeName));

        /// <summary>
        /// Materialize a <see cref="InMemoryKeyValueStore"/> with the given name.
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <returns>a new <see cref="InMemory{K, V}"/> instance with the given storeName</returns>
        public static InMemory<K, V> @As<KS, VS>(string storeName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new InMemory<K, V>(storeName, new InMemoryKeyValueBytesStoreSupplier(storeName))
            {
                KeySerdes = new KS(),
                ValueSerdes = new VS()
            };
            return m;
        }

    }

    /// <summary>
    /// <see cref="InMemoryWindows{K, V}"/> is a child class of <see cref="Materialized{K, V, S}"/>. 
    /// It's a class helper for materialize <see cref="IKTable{K, V}"/> with an <see cref="InMemoryTimestampedWindowStoreSupplier"/>
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">type of value</typeparam>
    public class InMemoryWindows<K, V> : Materialized<K, V, WindowStore<Bytes, byte[]>>
    {
        /// <summary>
        /// Protected constructor with state store name and supplier
        /// </summary>
        /// <param name="name">State store name for query it</param>
        /// <param name="supplier">Supplier use to build the state store</param>
        protected InMemoryWindows(string name, StoreSupplier<WindowStore<Bytes, byte[]>> supplier)
            : base(name, supplier)
        {

        }

        /// <summary>
        /// Materialize a <see cref="InMemoryWindowStore"/> with the given name.
        /// </summary>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <param name="windowSize">the windows size aggregation</param>
        /// <returns>a new <see cref="InMemoryWindows{K, V}"/> instance with the given storeName and windows size</returns>
        public static InMemoryWindows<K, V> @As(string storeName, TimeSpan windowSize)
            => new InMemoryWindows<K, V>(storeName, new InMemoryTimestampedWindowStoreSupplier(storeName, TimeSpan.FromDays(1), (long)windowSize.TotalMilliseconds));

        /// <summary>
        /// Materialize a <see cref="InMemoryWindowStore"/> with the given name.
        /// </summary>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the underlying <see cref="IKTable{K, V}"/> state store; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        /// <param name="windowSize">the windows size aggregation</param>
        /// <returns>a new <see cref="InMemoryWindows{K, V}"/> instance with the given storeName</returns>
        public static InMemoryWindows<K, V> @As<KS, VS>(string storeName, TimeSpan windowSize)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var m = new InMemoryWindows<K, V>(storeName, new InMemoryTimestampedWindowStoreSupplier(storeName, TimeSpan.FromDays(1), (long)windowSize.TotalMilliseconds))
            {
                KeySerdes = new KS(),
                ValueSerdes = new VS()
            };
            return m;
        }

    }

    #endregion
}
