using Kafka.Streams.Net.SerDes;
using Kafka.Streams.Net.State.Internal.Builder;
using Kafka.Streams.Net.State.Supplier;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State
{
    internal static class Stores
    {
        public static KeyValueBytesStoreSupplier PersistentTimestampedKeyValueStore(string name)
        {
            // TODO : RocksDB IMPLEMENTATION
            //return new RocksDbKeyValueBytesStoreSupplier(name, true);
            return null;
        }

        public static StoreBuilder<TimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(KeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            return new TimestampedKeyValueStoreBuilder<K, V>(supplier, keySerde, valueSerde);
        }
    }
}
