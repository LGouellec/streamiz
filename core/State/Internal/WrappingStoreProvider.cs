using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Provides a wrapper over multiple underlying <see cref="IStateStoreProvider"/>s
    /// </summary>
    internal class WrappingStoreProvider : IStateStoreProvider
    {
        private IEnumerable<IStateStoreProvider> storeProviders;

        public WrappingStoreProvider(IEnumerable<IStateStoreProvider> storeProviders)
        {
            this.storeProviders = storeProviders;
        }

        /// <summary>
        /// Provides access to <see cref="Processors.IStateStore"/>s accepted by <see cref="IQueryableStoreType{T}.Accepts(Processors.IStateStore)"/>
        /// </summary>
        /// <typeparam name="T">The type of the Store</typeparam>
        /// <param name="storeName">Name of the store</param>
        /// <param name="type">The <see cref="IQueryableStoreType{T}"/></param>
        /// <returns>a List of all the stores with the storeName and accepted by<see cref="IQueryableStoreType{T}.Accepts(Processors.IStateStore)"/></returns>
        public IEnumerable<T> Stores<T>(string storeName, IQueryableStoreType<T> type) where T : class//, IStateStore
        {
            var allStores = this.storeProviders
                .SelectMany(store => store.Stores(storeName, type));

            if (!allStores.Any())
            {
                throw new InvalidStateStoreException($"The state store, {storeName}, may have migrated to another instance.");
            }

            return allStores;
        }
    }
}
