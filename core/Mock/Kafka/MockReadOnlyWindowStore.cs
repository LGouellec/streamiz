using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockReadOnlyWindowStore<K, V> : IStateStore, IReadOnlyWindowStore<K, V>
    {
        private readonly IEnumerable<IStateStore> stores;
        
        public string Name => "MockWindowStore";
        public bool Persistent => false;
        public bool IsLocally => true;

        public bool IsOpen => true;
        
        public MockReadOnlyWindowStore(IEnumerable<IStateStore> stores)
        {
            this.stores = stores;
        }
        
        private IEnumerable<IReadOnlyWindowStore<K, V>> GetAllStores()
        {
            var readonlystores = stores
                .OfType<IReadOnlyWindowStore<K, V>>()
                .ToList();

            var timestamp = stores
                .OfType<ITimestampedWindowStore<K, V>>()
                .Select(s => new ReadOnlyWindowStoreFacade<K, V>(s));

            readonlystores.AddRange(timestamp);
            return readonlystores;
        }
        
        public void Init(ProcessorContext context, IStateStore root)
        {
            context.Register(root, null);
        }

        public void Flush()
        {
            // NOTHING
        }

        public void Close()
        {
            // NOTHING
        }
        
        public V Fetch(K key, long time)
        {
            foreach (var store in GetAllStores())
            {
                var result = store.Fetch(key, time);
                if (result != null)
                {
                    return result;
                }
            }
            return default;
        }

        public IWindowStoreEnumerator<V> Fetch(K key, DateTime from, DateTime to)
            => Fetch(key, from.GetMilliseconds(), to.GetMilliseconds());

        public IWindowStoreEnumerator<V> Fetch(K key, long from, long to)
        {
            foreach (var store in GetAllStores())
            {
                var it = store.Fetch(key, from, to);
                if (!it.MoveNext())
                {
                    it.Dispose();
                }
                else
                {
                    it.Reset();
                    return it;
                }
            }
            return new EmptyWindowStoreEnumerator<V>();
        }

        public IKeyValueEnumerator<Windowed<K>, V> All()
        {
            return new CompositeKeyValueEnumerator<Windowed<K>, V, IReadOnlyWindowStore<K, V>>(
                GetAllStores(),
                (store) => store.All());
        }

        public IKeyValueEnumerator<Windowed<K>, V> FetchAll(DateTime from, DateTime to)
        {
            return new CompositeKeyValueEnumerator<Windowed<K>, V, IReadOnlyWindowStore<K, V>>(
                GetAllStores(),
                (store) => store.FetchAll(from, to));
        }
    }
}