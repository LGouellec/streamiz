using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State.Internal.Builder
{
    internal class TimestampedKeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, ITimestampedKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier storeSupplier;

        public TimestampedKeyValueStoreBuilder(IKeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde) :
            base(supplier.Name, keySerde, valueSerde != null ? new ValueAndTimestampSerDes<V>(valueSerde) : null)
        {
            storeSupplier = supplier;
        }

        public override ITimestampedKeyValueStore<K, V> Build()
        {
            var store = storeSupplier.Get();
            return new TimestampedKeyValueStore<K, V>(store, keySerdes, valueSerdes);
        }
    }
}
