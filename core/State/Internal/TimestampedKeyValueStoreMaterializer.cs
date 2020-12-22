using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class TimestampedKeyValueStoreMaterializer<K, V>
    {
        private readonly Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized;

        public TimestampedKeyValueStoreMaterializer(Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            materialized = materializedInternal;
        }

        public StoreBuilder<ITimestampedKeyValueStore<K, V>> Materialize()
        {
            KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier)materialized.StoreSupplier;
            if (supplier == null)
            {
                supplier = Stores.DefaultKeyValueStore(materialized.StoreName);
            }

            StoreBuilder<ITimestampedKeyValueStore<K, V>> builder = Stores.TimestampedKeyValueStoreBuilder(
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
