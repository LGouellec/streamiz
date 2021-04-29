using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal.Builder;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State
{
    internal static class Stores
    {
        public static IKeyValueBytesStoreSupplier DefaultKeyValueStore(string name)
            => InMemoryKeyValueStore(name);

        public static IKeyValueBytesStoreSupplier PersistentKeyValueStore(string name)
            => new RocksDbKeyValueBytesStoreSupplier(name);

        public static IKeyValueBytesStoreSupplier InMemoryKeyValueStore(string name)
            => new InMemoryKeyValueBytesStoreSupplier(name);

        public static IWindowBytesStoreSupplier DefaultWindowStore(string name, TimeSpan retention, TimeSpan windowSize, long segmentInterval = 3600000)
            => InMemoryWindowStore(name, retention, windowSize);

        public static IWindowBytesStoreSupplier PersistentWindowStore(string name, TimeSpan retention, TimeSpan windowSize, long segmentInterval = 3600000)
            => new RocksDbWindowBytesStoreSupplier(name, retention, segmentInterval, (long)windowSize.TotalMilliseconds);

        public static IWindowBytesStoreSupplier InMemoryWindowStore(string name, TimeSpan retention, TimeSpan windowSize)
            => new InMemoryWindowStoreSupplier(name, retention, (long)windowSize.TotalMilliseconds);

        public static StoreBuilder<IWindowStore<K, V>> WindowStoreBuilder<K, V>(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => new WindowStoreBuilder<K, V>(supplier, keySerdes, valueSerdes);

        public static StoreBuilder<ITimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(IKeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            => new TimestampedKeyValueStoreBuilder<K, V>(supplier, keySerde, valueSerde);

        public static StoreBuilder<ITimestampedWindowStore<K, V>> TimestampedWindowStoreBuilder<K, V>(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            => new TimestampedWindowStoreBuilder<K, V>(supplier, keySerde, valueSerde);
    }
}