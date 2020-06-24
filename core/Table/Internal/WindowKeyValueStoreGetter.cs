using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class WindowKeyValueStoreGetter<K, V> : IKTableValueGetter<Windowed<K>, V>
    {
        private readonly string storeName;
        private TimestampedWindowStore<K, V> store;

        public WindowKeyValueStoreGetter(string storeName)
        {
            this.storeName = storeName;
        }

        public void Close() { }

        public ValueAndTimestamp<V> Get(Windowed<K> key)
        {
            return store.Fetch(key.Key, key.Window.StartMs);
        }

        public void Init(ProcessorContext context)
        {
            store = (TimestampedWindowStore<K, V>)context.GetStateStore(storeName);
        }
    }
}
