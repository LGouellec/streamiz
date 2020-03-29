using kafka_stream_core.SerDes;
using kafka_stream_core.State.Internal.Builder;
using kafka_stream_core.State.Supplier;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    internal static class Stores
    {
        public static KeyValueBytesStoreSupplier persistentTimestampedKeyValueStore(string name)
        {
            //return new RocksDbKeyValueBytesStoreSupplier(name, true);
            return null;
        }

        public static StoreBuilder<kafka_stream_core.State.TimestampedKeyValueStore<K, V>> timestampedKeyValueStoreBuilder<K, V>(KeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            return new TimestampedKeyValueStoreBuilder<K, V>(supplier, keySerde, valueSerde, DateTime.Now);
        }

    }
}
