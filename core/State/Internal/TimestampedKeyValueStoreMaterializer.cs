using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.State.Supplier;
using kafka_stream_core.Table;

namespace kafka_stream_core.State.Internal
{
    internal class TimestampedKeyValueStoreMaterializer<K, V>
    {
        private Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized;

        public TimestampedKeyValueStoreMaterializer(Materialized<K, V, KeyValueStore<byte[], byte[]>> materializedInternal)
        {
            this.materialized = materializedInternal;
        }

        public StoreBuilder<TimestampedKeyValueStore<K, V>> materialize()
        {
            KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier)materialized.StoreSupplier;
            if (supplier == null)
            {
                String name = materialized.StoreName;
                supplier = Stores.persistentTimestampedKeyValueStore(name);
            }

            StoreBuilder<TimestampedKeyValueStore< K, V >> builder = Stores.timestampedKeyValueStoreBuilder(
                 supplier,
                 materialized.KeySerdes,
                 materialized.ValueSerdes);

            if (materialized.LoggingEnabled)
            {
                builder.withLoggingEnabled(materialized.TopicConfig);
            }
            else
            {
                builder.withLoggingDisabled();
            }

            if (materialized.CachingEnabled)
            {
                builder.withCachingEnabled();
            }
            
            return builder;
        }
    }
}
