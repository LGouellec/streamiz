using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.State;

namespace kafka_stream_core.Table.Internal
{
    internal class KTableMaterializedValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        internal class KTableMaterializedValueGetter : IKTableValueGetter<K, V>
        {
            private readonly string storeName;
            private TimestampedKeyValueStore<K, V> store;

            public KTableMaterializedValueGetter(string storeName)
            {
                this.storeName = storeName;
            }

            public void close(){}

            public ValueAndTimestamp<V> get(K key) => store.get(key);

            public void init(ProcessorContext context)
            {
                store = (TimestampedKeyValueStore<K, V>)context.GetStateStore(storeName);
            }
        }

        private string queryableStoreName;

        public KTableMaterializedValueGetterSupplier(string queryableStoreName)
        {
            this.queryableStoreName = queryableStoreName;
        }

        public string[] StoreNames => new string[1] { queryableStoreName };

        public IKTableValueGetter<K, V> get() => new KTableMaterializedValueGetter(this.queryableStoreName);
    }
}
