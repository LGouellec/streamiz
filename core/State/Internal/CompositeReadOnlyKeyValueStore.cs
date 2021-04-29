using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class CompositeReadOnlyKeyValueStore<K, V> : IReadOnlyKeyValueStore<K, V>
    {
        private readonly IStateStoreProvider<IReadOnlyKeyValueStore<K, V>, K, V> storeProvider;
        private readonly IQueryableStoreType<IReadOnlyKeyValueStore<K, V>, K, V> storeType;
        private readonly string storeName;

        public CompositeReadOnlyKeyValueStore(
            IStateStoreProvider<IReadOnlyKeyValueStore<K, V>, K, V> storeProvider,
            IQueryableStoreType<IReadOnlyKeyValueStore<K, V>, K, V> storeType, string storeName)
        {
            this.storeProvider = storeProvider;
            this.storeType = storeType;
            this.storeName = storeName;
        }

        public IEnumerable<KeyValuePair<K, V>> All()
        {
            // TODO: implement DelegatingPeekingKeyValueIterator

            IEnumerable<IReadOnlyKeyValueStore<K, V>> stores = GetAllStores();
            try
            {
                return stores.SelectMany(x =>
                {
                    return x.All();
                });
            }
            catch (InvalidStateStoreException e)
            {
                // TODO is there a point in doing this?
                throw new InvalidStateStoreException("State store is not available anymore and may have " +
                    "been migrated to another instance; please re-discover its location from the state metadata.", e);
            }
        }

        public long ApproximateNumEntries()
        {
            IEnumerable<IReadOnlyKeyValueStore<K, V>> stores = GetAllStores();
            long result = 0;
            foreach (var store in stores)
            {
                result += store.ApproximateNumEntries();
                if (result < 0)
                {
                    return long.MaxValue;
                }
            }
            return result;
        }

        public V Get(K key)
        {
            IEnumerable<IReadOnlyKeyValueStore<K, V>> stores = GetAllStores();
            try
            {
                return stores.FirstOrDefault(x => x.Get(key) != null).Get(key);
            }
            catch (InvalidStateStoreException e)
            {
                // TODO is there a point in doing this?
                throw new InvalidStateStoreException("State store is not available anymore and may have " +
                    "been migrated to another instance; please re-discover its location from the state metadata.", e);
            }
        }

        public IKeyValueEnumerator<K, V> Range(K from, K to)
        {
            IEnumerable<IReadOnlyKeyValueStore<K, V>> stores = GetAllStores();
            try
            {
                return new CompositeKeyValueEnumerator<K, V>(
                    stores.Select(x => x.Range(from, to)));
            }
            catch (InvalidStateStoreException e)
            {
                // TODO is there a point in doing this?
                throw new InvalidStateStoreException("State store is not available anymore and may have " +
                    "been migrated to another instance; please re-discover its location from the state metadata.", e);
            }
        }

        private IEnumerable<IReadOnlyKeyValueStore<K, V>> GetAllStores()
        {
            return storeProvider.Stores(storeName, storeType);
        }
    }
}
