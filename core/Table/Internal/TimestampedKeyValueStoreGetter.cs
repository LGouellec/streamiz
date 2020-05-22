using System;
using System.Collections.Generic;
using System.Text;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class TimestampedKeyValueStoreGetter<K, V> : IKTableValueGetter<K, V>
    {
        private readonly string storeName;
        private TimestampedKeyValueStore<K, V> store;

        public TimestampedKeyValueStoreGetter(string storeName)
        {
            this.storeName = storeName;
        }

        public void Close() { }

        public ValueAndTimestamp<V> Get(K key) 
            => store.Get(key);

        public void Init(ProcessorContext context) =>
            store = (TimestampedKeyValueStore<K, V>)context.GetStateStore(storeName);
    }
}
