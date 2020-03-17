using kafka_stream_core.State;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal
{
    internal class KTableSourceValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        internal class KTableSourceValueGetter : IKTableValueGetter<K, V>
        {
            private readonly string storeName;
            private TimestampedKeyValueStore<K, V> store;

            public KTableSourceValueGetter(string storeName)
            {
                this.storeName = storeName;
            }

            public void close() { }

            public ValueAndTimestamp<V> get(K key) => store.get(key);

            public void init(ProcessorContext context)
            {
                store = (TimestampedKeyValueStore<K, V>)context.GetStateStore(storeName);
            }

        }

        private string queryableStoreName;

        public KTableSourceValueGetterSupplier(string queryableStoreName)
        {
            this.queryableStoreName = queryableStoreName;
        }

        public string[] StoreNames => new string[1] { queryableStoreName };

        public IKTableValueGetter<K, V> get() => new KTableSourceValueGetter(this.queryableStoreName);
    }
}
