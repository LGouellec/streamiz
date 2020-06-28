using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Enumerator;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class CompositeReadOnlyWindowStore<K, V> : ReadOnlyWindowStore<K, V>
    {
        private readonly IStateStoreProvider<ReadOnlyWindowStore<K, V>, K, V> storeProvider;
        private readonly IQueryableStoreType<ReadOnlyWindowStore<K, V>, K, V> storeType;
        private readonly string storeName;

        public CompositeReadOnlyWindowStore(IStateStoreProvider<ReadOnlyWindowStore<K, V>, K, V> storeProvider, WindowStoreType<K, V> storeType, string storeName)
        {
            this.storeProvider = storeProvider;
            this.storeType = storeType;
            this.storeName = storeName;
        }

        public IKeyValueEnumerator<Windowed<K>, V> All()
        {
            return new CompositeKeyValueEnumerator<Windowed<K>, V, ReadOnlyWindowStore<K, V>>(
                storeProvider.Stores(storeName, storeType),
                (store) => store.All());
        }

        public V Fetch(K key, long time)
        {
            var stores = GetAllStores();
            try
            {
                foreach (var store in stores)
                {
                    var result = store.Fetch(key, time);
                    if (result != null)
                    {
                        return result;
                    }
                }
                return default;
            }
            catch (InvalidStateStoreException e)
            {
                // TODO is there a point in doing this?
                throw new InvalidStateStoreException("State store is not available anymore and may have " +
                    "been migrated to another instance; please re-discover its location from the state metadata.", e);
            }
        }

        public IWindowStoreEnumerator<V> Fetch(K key, DateTime from, DateTime to)
        {
            var stores = GetAllStores();
            try
            {
                foreach (var store in stores)
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
                // TODO : Empty enumerator
                return null;
            }
            catch (InvalidStateStoreException e)
            {
                // TODO is there a point in doing this?
                throw new InvalidStateStoreException("State store is not available anymore and may have " +
                    "been migrated to another instance; please re-discover its location from the state metadata.", e);
            }
        }

        public IKeyValueEnumerator<Windowed<K>, V> FetchAll(DateTime from, DateTime to)
        {
            return new CompositeKeyValueEnumerator<Windowed<K>, V, ReadOnlyWindowStore<K, V>>(
                storeProvider.Stores(storeName, storeType),
                (store) => store.FetchAll(from, to));
        }

        private IEnumerable<ReadOnlyWindowStore<K, V>> GetAllStores()
        {
            return storeProvider.Stores(storeName, storeType);
        }
    }
}
