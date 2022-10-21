using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Logging;
using Streamiz.Kafka.Net.State.Metered;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    public class KeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, V, IKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier supplier;

        public KeyValueStoreBuilder(IKeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde) 
            : base(supplier.Name, keySerde, valueSerde)
        {
            this.supplier = supplier;
        }

        public override bool IsWindowStore => false;
        public override long RetentionMs => throw new IllegalStateException("RetentionMs is not supported when not a window store");
        public override IKeyValueStore<K, V> Build()
        {
            var store = supplier.Get();

            return new MeteredKeyValueStore<K, V>(
                WrapLogging(store),
                keySerdes,
                valueSerdes,
                supplier.MetricsScope);
        }
        
        private IKeyValueStore<Bytes, byte[]> WrapLogging(IKeyValueStore<Bytes, byte[]> inner)
        {
            if (!LoggingEnabled)
                return inner;

            return new ChangeLoggingKeyValueBytesStore(inner);
        }
    }
}