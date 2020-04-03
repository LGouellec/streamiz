using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.SerDes;
using kafka_stream_core.State.Supplier;

namespace kafka_stream_core.State.Internal.Builder
{
    internal class TimestampedKeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, kafka_stream_core.State.TimestampedKeyValueStore<K, V>>
    {
        private readonly KeyValueBytesStoreSupplier storeSupplier;

        public TimestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde) :
            base(supplier.Name, keySerde, valueSerde != null ? new ValueAndTimestampSerDes<V>(valueSerde) : null)
        {
            this.storeSupplier = supplier;
        }

        public override kafka_stream_core.State.TimestampedKeyValueStore<K, V> Build()
        {
            var store = storeSupplier.Get();
            return new TimestampedKeyValueStore<K, V>(store, this.keySerdes, this.valueSerdes);
        }
    }
}
