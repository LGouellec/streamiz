using Streamiz.Kafka.Net.Errors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class CompositeReadOnlyKeyValueStore<K, V> : ReadOnlyKeyValueStore<K, V>
    {
        private readonly IStateStoreProvider<ReadOnlyKeyValueStore<K, V>, K, V> storeProvider;
        private readonly IQueryableStoreType<ReadOnlyKeyValueStore<K, V>, K, V> storeType;
        private readonly string storeName;

        public CompositeReadOnlyKeyValueStore(
            IStateStoreProvider<ReadOnlyKeyValueStore<K, V>, K, V> storeProvider,
            IQueryableStoreType<ReadOnlyKeyValueStore<K, V>, K, V> storeType, string storeName)
        {
            this.storeProvider = storeProvider;
            this.storeType = storeType;
            this.storeName = storeName;
        }

        public IEnumerable<KeyValuePair<K, V>> All()
        {
            // TODO: implement DelegatingPeekingKeyValueIterator

            IEnumerable<ReadOnlyKeyValueStore<K, V>> stores = GetAllStores();
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
            IEnumerable<ReadOnlyKeyValueStore<K, V>> stores = GetAllStores();
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
            IEnumerable<ReadOnlyKeyValueStore<K, V>> stores = GetAllStores();
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

        private IEnumerable<ReadOnlyKeyValueStore<K, V>> GetAllStores()
        {
            return storeProvider.Stores(storeName, storeType);
        }
    }
}
