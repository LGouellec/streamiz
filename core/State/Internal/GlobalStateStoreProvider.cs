using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class GlobalStateStoreProvider
    {
        private readonly IDictionary<string, IStateStore> globalStateStores;

        public GlobalStateStoreProvider(IDictionary<string, IStateStore> globalStateStores)
        {
            this.globalStateStores = globalStateStores;
        }

        public IEnumerable<T> Stores<T, K, V>(StoreQueryParameters<T, K, V> storeQueryParameters)
            where T : class
        {
            string storeName = storeQueryParameters.StoreName;
            IQueryableStoreType<T, K, V> queryableStoreType = storeQueryParameters.QueryableStoreType;
            IStateStore stateStore;
            if (!globalStateStores.TryGetValue(storeName, out stateStore) || !queryableStoreType.Accepts(stateStore))
            {
                return Enumerable.Empty<T>();
            }
            
            if (!stateStore.IsOpen)
            {
                throw new InvalidStateStoreException($"the state store, {storeName}, is not open.");
            }
            if (stateStore is TimestampedKeyValueStore<K, V> && queryableStoreType is KeyValueStoreType<K, V>)
            {
                return new[] { new ReadOnlyKeyValueStoreFacade<K, V>(stateStore as TimestampedKeyValueStore<K, V>) as T };
            }
            // TODO: handle TimestampedWindowStore 
            //} else if (store instanceof TimestampedWindowStore && queryableStoreType instanceof QueryableStoreTypes.WindowStoreType) {
            //    return (List<T>) Collections.singletonList(new ReadOnlyWindowStoreFacade((TimestampedWindowStore<Object, Object>) store));
            //}

            return new[] { stateStore as T };
        }
    }
}
