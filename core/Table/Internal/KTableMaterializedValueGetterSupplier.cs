using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Streams.Net.State;

namespace Kafka.Streams.Net.Table.Internal
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

            public void Close(){}

            public ValueAndTimestamp<V> Get(K key) => store.Get(key);

            public void Init(ProcessorContext context)
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

        public IKTableValueGetter<K, V> Get() => new KTableMaterializedValueGetter(this.queryableStoreName);
    }
}
