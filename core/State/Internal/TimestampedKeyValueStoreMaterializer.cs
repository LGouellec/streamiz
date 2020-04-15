using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.State.InMemory;
using Kafka.Streams.Net.State.Supplier;
using Kafka.Streams.Net.Table;

namespace Kafka.Streams.Net.State.Internal
{
    internal class TimestampedKeyValueStoreMaterializer<K, V>
    {
        private Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized;

        public TimestampedKeyValueStoreMaterializer(Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            this.materialized = materializedInternal;
        }

        public StoreBuilder<TimestampedKeyValueStore<K, V>> Materialize()
        {
            KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier)materialized.StoreSupplier;
            if (supplier == null)
            {
                String name = materialized.StoreName;
                // TODO : RocksDB
                //supplier = Stores.persistentTimestampedKeyValueStore(name);
                supplier = new InMemoryKeyValueBytesStoreSupplier(name);
            }

            StoreBuilder<TimestampedKeyValueStore< K, V >> builder = Stores.TimestampedKeyValueStoreBuilder(
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
