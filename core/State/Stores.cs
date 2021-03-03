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
        public static KeyValueBytesStoreSupplier DefaultKeyValueStore(string name)
            => InMemoryKeyValueStore(name);

        public static KeyValueBytesStoreSupplier PersistentKeyValueStore(string name)
            => new RocksDbKeyValueBytesStoreSupplier(name);

        public static KeyValueBytesStoreSupplier InMemoryKeyValueStore(string name)
            => new InMemoryKeyValueBytesStoreSupplier(name);

        public static WindowBytesStoreSupplier DefaultWindowStore(string name, TimeSpan retention, TimeSpan windowSize)
            => InMemoryWindowStore(name, retention, windowSize);

        public static WindowBytesStoreSupplier PersistentWindowStore(string name, TimeSpan retention, TimeSpan windowSize)
        {
            // TODO:
            return null;
        }

        public static WindowBytesStoreSupplier InMemoryWindowStore(string name, TimeSpan retention, TimeSpan windowSize)
            => new InMemoryWindowStoreSupplier(name, retention, (long)windowSize.TotalMilliseconds);

        public static StoreBuilder<IWindowStore<K, V>> WindowStoreBuilder<K, V>(WindowBytesStoreSupplier supplier, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => new WindowStoreBuilder<K, V>(supplier, keySerdes, valueSerdes);

        public static StoreBuilder<ITimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(KeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            => new TimestampedKeyValueStoreBuilder<K, V>(supplier, keySerde, valueSerde);

        public static StoreBuilder<ITimestampedWindowStore<K, V>> TimestampedWindowStoreBuilder<K, V>(WindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            => new TimestampedWindowStoreBuilder<K, V>(supplier, keySerde, valueSerde);
    }
}