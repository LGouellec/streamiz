using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Logging;
using Streamiz.Kafka.Net.State.Metered;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    public class TimestampedWindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, ITimestampedWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier supplier;

        public TimestampedWindowStoreBuilder(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            : base(supplier.Name, keySerde, valueSerde == null ? null : new ValueAndTimestampSerDes<V>(valueSerde))
        {
            this.supplier = supplier;
        }

        public override bool IsWindowStore => true;

        public override long RetentionMs => supplier.Retention;

        public override ITimestampedWindowStore<K, V> Build()
        {
            var store = supplier.Get();
            return new MeteredTimestampedWindowStore<K, V>(
                WrapLogging(store),
                supplier.WindowSize.Value,
                keySerdes,
                valueSerdes,
                supplier.MetricsScope);
        }

        private IWindowStore<Bytes, byte[]> WrapLogging(IWindowStore<Bytes, byte[]> inner)
        {
            if (!LoggingEnabled)
                return inner;

            return new ChangeLoggingTimestampedWindowBytesStore(inner);
        }
    }
}
