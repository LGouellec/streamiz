using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockReadOnlyKeyValueStore<K, V> : IStateStore, ReadOnlyKeyValueStore<K, V>
    {
        private readonly IEnumerable<IStateStore> stores;

        public string Name => "MockStore";

        public bool Persistent => false;

        public bool IsOpen => true;

        public MockReadOnlyKeyValueStore(IEnumerable<IStateStore> stores)
        {
            this.stores = stores;
        }

        private IEnumerable<ReadOnlyKeyValueStore<K, V>> GetAllStores()
        {
            var readonlystores = stores
                .Where(s => s is ReadOnlyKeyValueStore<K, V>)
                .Select(s => s as ReadOnlyKeyValueStore<K, V>).ToList();

            var timestamp = stores
                .Where(s => s is TimestampedKeyValueStore<K, V>)
                .Select(s => new ReadOnlyKeyValueStoreFacade<K, V>(s as TimestampedKeyValueStore<K, V>));
           
            readonlystores.AddRange(timestamp);
            return readonlystores;
        }

        public IEnumerable<KeyValuePair<K, V>> All() => GetAllStores().SelectMany(x => x.All());

        public long ApproximateNumEntries() => GetAllStores().Sum(x => x.ApproximateNumEntries());

        public V Get(K key)
        {
            IEnumerable<ReadOnlyKeyValueStore<K, V>> stores = this.GetAllStores();
            var item = stores.FirstOrDefault(x => x.Get(key) != null);
            return item != null ? item.Get(key) : default;
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            
        }

        public void Flush()
        {
            
        }

        public void Close()
        {
            
        }
    }
}
