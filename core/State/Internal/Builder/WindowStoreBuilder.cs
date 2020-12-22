using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State.Internal.Builder
{
    internal class WindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, V, IWindowStore<K, V>>
    {
        private readonly WindowBytesStoreSupplier supplier;

        public WindowStoreBuilder(WindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde) 
            : base(supplier.Name, keySerde, valueSerde)
        {
            this.supplier = supplier;
        }

        public override IWindowStore<K, V> Build()
            => new WrappedWindowStore<K, V>(supplier.Get(), supplier.WindowSize.Value, keySerdes, valueSerdes);
    }
}
