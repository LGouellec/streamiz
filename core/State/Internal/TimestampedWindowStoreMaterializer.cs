using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class TimestampedWindowStoreMaterializer<K, V, W> : 
        WindowStoreMaterializer<K, V, W, ITimestampedWindowStore<K, V>>
        where W : Window
    {
        public TimestampedWindowStoreMaterializer(WindowOptions<W> windowsOptions, Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal)
            : base(windowsOptions, materializedInternal)
        {
            
        }

        protected override IStoreBuilder<ITimestampedWindowStore<K, V>> GetWindowStoreBuilder(IWindowBytesStoreSupplier supplier)
            => Stores.TimestampedWindowStoreBuilder(supplier, materializedInternal.KeySerdes, materializedInternal.ValueSerdes);
   }
}
