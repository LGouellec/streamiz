using Kafka.Streams.Net.SerDes;
using Kafka.Streams.Net.State.Supplier;

namespace Kafka.Streams.Net.State.Internal.Builder
{
    internal class TimestampedKeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>
    {
        private readonly KeyValueBytesStoreSupplier storeSupplier;

        public TimestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde) :
            base(supplier.Name, keySerde, valueSerde != null ? new ValueAndTimestampSerDes<V>(valueSerde) : null)
        {
            this.storeSupplier = supplier;
        }

        public override TimestampedKeyValueStore<K, V> Build()
        {
            var store = storeSupplier.Get();
            return new TimestampedKeyValueStoreImpl<K, V>(store, this.keySerdes, this.valueSerdes);
        }
    }
}
