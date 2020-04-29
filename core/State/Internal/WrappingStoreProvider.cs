using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Provides a wrapper over multiple underlying <see cref="IStateStoreProvider{T, K, V}"/>s
    /// </summary>
    /// <typeparam name="T">The type of the store to be provided</typeparam>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    internal class WrappingStoreProvider<T, K, V> : IStateStoreProvider<T, K, V> 
        where T : class
    {
        private IEnumerable<StreamThreadStateStoreProvider> storeProviders;
        private readonly StoreQueryParameters<T, K, V> storeQueryParameters;

        public WrappingStoreProvider(IEnumerable<StreamThreadStateStoreProvider> storeProviders, StoreQueryParameters<T, K, V> storeQueryParameters)
        {
            this.storeProviders = storeProviders;
            this.storeQueryParameters = storeQueryParameters;
        }

        /// <summary>
        /// Provides access to <see cref="Processors.IStateStore"/>s accepted by <see cref="IQueryableStoreType{T, K, V}.Accepts(IStateStore)"/>
        /// </summary>
        /// <param name="storeName">Name of the store</param>
        /// <param name="type">The <see cref="IQueryableStoreType{T, K, V}"/></param>
        /// <returns>a List of all the stores with the storeName and accepted by<see cref="IQueryableStoreType{T, K, V}.Accepts(IStateStore)"/></returns>
        public IEnumerable<T> Stores(string storeName, IQueryableStoreType<T, K, V> type)
        {
            var allStores = this.storeProviders
                .SelectMany(store => store.Stores(storeQueryParameters));

            if (!allStores.Any())
            {
                throw new InvalidStateStoreException($"The state store, {storeName}, may have migrated to another instance.");
            }

            return allStores;
        }
    }
}
