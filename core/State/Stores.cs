using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Helper class for creating state store.
    /// </summary>
    public static class Stores
    {
        /// <summary>
        /// Create the default key/value store. The default state store is an <see cref="Streamiz.Kafka.Net.State.InMemory.InMemoryKeyValueStore"/>.
        /// </summary>
        /// <param name="name">state store name</param>
        /// <returns><see cref="InMemoryKeyValueBytesStoreSupplier"/> supplier</returns>
        public static IKeyValueBytesStoreSupplier DefaultKeyValueStore(string name)
            => PersistentKeyValueStore(name);

        /// <summary>
        /// Create a persistent key/value store.
        /// </summary>
        /// <param name="name">state store name</param>
        /// <returns><see cref="RocksDbKeyValueBytesStoreSupplier"/> supplier</returns>
        public static IKeyValueBytesStoreSupplier PersistentKeyValueStore(string name)
            => new RocksDbKeyValueBytesStoreSupplier(name);

        /// <summary>
        /// Create a inmemory key/value store.
        /// </summary>
        /// <param name="name">state store name</param>
        /// <returns><see cref="InMemoryKeyValueBytesStoreSupplier"/> supplier</returns>
        public static IKeyValueBytesStoreSupplier InMemoryKeyValueStore(string name)
            => new InMemoryKeyValueBytesStoreSupplier(name);

        /// <summary>
        /// Create the default window store. The default state store is an <see cref="Streamiz.Kafka.Net.State.InMemory.InMemoryWindowStore"/>. 
        /// </summary>
        /// <param name="name">state store name</param>
        /// <param name="retention">retention duration</param>
        /// <param name="windowSize">window size</param>
        /// <param name="segmentInterval">segment interval</param>
        /// <param name="retainDuplicates"> whether or not to retain duplicates. Turning this on will automatically disable caching and means that null values will be ignored.</param>
        /// <returns><see cref="InMemoryWindowStoreSupplier"/> supplier</returns>
        public static IWindowBytesStoreSupplier DefaultWindowStore(string name, TimeSpan retention, TimeSpan windowSize, long segmentInterval = 3600000, bool retainDuplicates = false)
            => PersistentWindowStore(name, retention, windowSize, segmentInterval, retainDuplicates);

        /// <summary>
        /// Create a persistent window store. 
        /// </summary>
        /// <param name="name">state store name</param>
        /// <param name="retention">retention duration</param>
        /// <param name="windowSize">window size</param>
        /// <param name="segmentInterval">segment interval (default: 3600000)</param>
        /// <param name="retainDuplicates"> whether or not to retain duplicates. Turning this on will automatically disable caching and means that null values will be ignored.</param>
        /// <returns><see cref="RocksDbWindowBytesStoreSupplier"/> supplier</returns>
        public static IWindowBytesStoreSupplier PersistentWindowStore(string name, TimeSpan retention, TimeSpan windowSize, long segmentInterval = 3600000, bool retainDuplicates = false)
            => new RocksDbWindowBytesStoreSupplier(name, retention, segmentInterval, (long)windowSize.TotalMilliseconds, retainDuplicates);

        /// <summary>
        /// Create the inmemory window store. 
        /// </summary>
        /// <param name="name">state store name</param>
        /// <param name="retention">retention duration</param>
        /// <param name="windowSize">window size</param>
        /// <param name="retainDuplicates"> whether or not to retain duplicates. Turning this on will automatically disable caching and means that null values will be ignored.</param>
        /// <returns><see cref="InMemoryWindowStoreSupplier"/> supplier</returns>
        public static IWindowBytesStoreSupplier InMemoryWindowStore(string name, TimeSpan retention, TimeSpan windowSize, bool retainDuplicates = false)
            => new InMemoryWindowStoreSupplier(name, retention, (long)windowSize.TotalMilliseconds, retainDuplicates);

        /// <summary>
        /// Creates a <see cref="IStoreBuilder"/> that can be used to build a <see cref="IWindowStore{K, V}"/>.
        /// </summary>
        /// <param name="supplier">a <see cref="IWindowBytesStoreSupplier"/> (cannot be null)</param>
        /// <param name="keySerdes">the key serde to use</param>
        /// <param name="valueSerdes">the value serde to use</param>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <returns>an instance of <see cref="IStoreBuilder"/> than can build a <see cref="IWindowStore{K, V}"/></returns>
        public static IStoreBuilder<IWindowStore<K, V>> WindowStoreBuilder<K, V>(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => new WindowStoreBuilder<K, V>(supplier, keySerdes, valueSerdes);

        /// <summary>
        /// Creates a <see cref="IStoreBuilder"/> that can be used to build a <see cref="ITimestampedKeyValueStore{K, V}"/>.
        /// </summary>
        /// <param name="supplier">a <see cref="IKeyValueBytesStoreSupplier"/> (cannot be null)</param>
        /// <param name="keySerde">the key serde to use</param>
        /// <param name="valueSerde">the value serde to use</param>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <returns>an instance of <see cref="IStoreBuilder"/> than can build a <see cref="ITimestampedKeyValueStore{K, V}"/></returns>
        public static IStoreBuilder<ITimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(
            IKeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            => new TimestampedKeyValueStoreBuilder<K, V>(supplier, keySerde, valueSerde);

        /// <summary>
        /// Creates a <see cref="IStoreBuilder"/> that can be used to build a <see cref="ITimestampedWindowStore{K, V}"/>.
        /// </summary>
        /// <param name="supplier">a <see cref="IWindowBytesStoreSupplier"/> (cannot be null)</param>
        /// <param name="keySerde">the key serde to use</param>
        /// <param name="valueSerde">the value serde to use</param>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <returns>an instance of <see cref="IStoreBuilder"/> than can build a <see cref="ITimestampedWindowStore{K, V}"/></returns>
        public static IStoreBuilder<ITimestampedWindowStore<K, V>> TimestampedWindowStoreBuilder<K, V>(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            => new TimestampedWindowStoreBuilder<K, V>(supplier, keySerde, valueSerde);
        
        /// <summary>
        /// Creates a <see cref="IStoreBuilder"/> that can be used to build a <see cref="IKeyValueStore{K, V}"/>.
        /// </summary>
        /// <param name="supplier">a <see cref="IKeyValueBytesStoreSupplier"/> (cannot be null)</param>
        /// <param name="keySerde">the key serde to use</param>
        /// <param name="valueSerde">the value serde to use</param>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <returns>an instance of <see cref="IStoreBuilder"/> than can build a <see cref="IKeyValueStore{K, V}"/></returns>
        public static IStoreBuilder<IKeyValueStore<K, V>> KeyValueStoreBuilder<K, V>(IKeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            => new KeyValueStoreBuilder<K, V>(supplier, keySerde, valueSerde);
    }
}