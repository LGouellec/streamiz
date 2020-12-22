using System;
using System.Collections.Generic;
using System.Text;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State.Internal.Builder
{
    internal class TimestampedWindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, ITimestampedWindowStore<K, V>>
    {
        private readonly WindowBytesStoreSupplier supplier;

        public TimestampedWindowStoreBuilder(WindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            : base(supplier.Name, keySerde, valueSerde == null ? null : new ValueAndTimestampSerDes<V>(valueSerde))
        {
            this.supplier = supplier;
        }

        public override ITimestampedWindowStore<K, V> Build()
        {
            var store = supplier.Get();
            return new TimestampedWindowStoreImpl<K, V>(store, supplier.WindowSize.Value, keySerdes, valueSerdes);
        }
    }
}
