using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.State.InMemory;
using kafka_stream_core.State.Supplier;
using kafka_stream_core.Table;

namespace kafka_stream_core.State.Internal
{
    internal class TimestampedKeyValueStoreMaterializer<K, V>
    {
        private Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized;

        public TimestampedKeyValueStoreMaterializer(Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            this.materialized = materializedInternal;
        }

        public StoreBuilder<kafka_stream_core.State.TimestampedKeyValueStore<K, V>> Materialize()
        {
            KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier)materialized.StoreSupplier;
            if (supplier == null)
            {
                String name = materialized.StoreName;
                // TODO : RocksDB
                //supplier = Stores.persistentTimestampedKeyValueStore(name);
                supplier = new InMemoryKeyValueBytesStoreSupplier(name);
            }

            StoreBuilder<kafka_stream_core.State.TimestampedKeyValueStore< K, V >> builder = Stores.TimestampedKeyValueStoreBuilder(
                 supplier,
                 materialized.KeySerdes,
                 materialized.ValueSerdes);

            if (materialized.LoggingEnabled)
            {
                builder.WithLoggingEnabled(materialized.TopicConfig);
            }
            else
            {
                builder.WithLoggingDisabled();
            }

            if (materialized.CachingEnabled)
            {
                builder.WithCachingEnabled();
            }
            
            return builder;
        }
    }
}
