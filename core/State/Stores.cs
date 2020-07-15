using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal.Builder;
using Streamiz.Kafka.Net.State.Supplier;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State
{
    internal static class Stores
    {
        public static KeyValueBytesStoreSupplier PersistentKeyValueStore(string name)
        {
            // TODO : RocksDB IMPLEMENTATION
            //return new RocksDbKeyValueBytesStoreSupplier(name, true);
            return null;
        }

        public static KeyValueBytesStoreSupplier InMemoryKeyValueStore(string name)
        {
            return new InMemoryKeyValueBytesStoreSupplier(name);
        }

        public static WindowBytesStoreSupplier InMemoryWindowStore(string name, TimeSpan retention, TimeSpan windowSize)
        {
            return new InMemoryWindowStoreSupplier(name, retention, (long)windowSize.TotalMilliseconds);
        }

        public static StoreBuilder<TimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(KeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            return new TimestampedKeyValueStoreBuilder<K, V>(supplier, keySerde, valueSerde);
        } 
        
        public static StoreBuilder<TimestampedWindowStore<K, V>> TimestampedWindowStoreBuilder<K, V>(WindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            return new TimestampedWindowStoreBuilder<K, V>(supplier, keySerde, valueSerde);
        }

        public static StoreBuilder<WindowStore<K, V>> WindowStoreBuilder<K, V>(WindowBytesStoreSupplier supplier, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            return new WindowStoreBuilder<K, V>(supplier, keySerdes, valueSerdes);
        }
    }
}
