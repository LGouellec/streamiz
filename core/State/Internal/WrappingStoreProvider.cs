using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Provides a wrapper over multiple underlying <see cref="IStateStoreProvider{T}"/>s
    /// </summary>
    /// <typeparam name="T">The type of the store to be provided</typeparam>
    internal class WrappingStoreProvider<T> : IStateStoreProvider<T> where T : class
    {
        private IEnumerable<StreamThreadStateStoreProvider> storeProviders;
        private readonly StoreQueryParameters<T> storeQueryParameters;

        public WrappingStoreProvider(IEnumerable<StreamThreadStateStoreProvider> storeProviders, StoreQueryParameters<T> storeQueryParameters)
        {
            this.storeProviders = storeProviders;
            this.storeQueryParameters = storeQueryParameters;
        }

        /// <summary>
        /// Provides access to <see cref="Processors.IStateStore"/>s accepted by <see cref="IQueryableStoreType{T}.Accepts(Processors.IStateStore)"/>
        /// </summary>
        /// <param name="storeName">Name of the store</param>
        /// <param name="type">The <see cref="IQueryableStoreType{T}"/></param>
        /// <returns>a List of all the stores with the storeName and accepted by<see cref="IQueryableStoreType{T}.Accepts(Processors.IStateStore)"/></returns>
        public IEnumerable<T> Stores(string storeName, IQueryableStoreType<T> type)
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
