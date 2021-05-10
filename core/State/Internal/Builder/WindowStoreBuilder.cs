using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State.Internal.Builder
{
    internal class WindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, V, IWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier supplier;

        public WindowStoreBuilder(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde) 
            : base(supplier.Name, keySerde, valueSerde)
        {
            this.supplier = supplier;
        }

        public override bool IsWindowStore => true;

        public override long RetentionMs => supplier.Retention;

        public override IWindowStore<K, V> Build()
            => new WrappedWindowStore<K, V>(supplier.Get(), supplier.WindowSize.Value, keySerdes, valueSerdes);
    }
}
