using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// A wrapper over all of the <see cref="IStateStoreProvider"/>s in a Topology
    /// </summary>
    internal class QueryableStoreProvider
    {
        // TODO: uncomment GlobalStateStoreProvider code when it is available

        private readonly IEnumerable<IStateStoreProvider> storeProviders;
        //private readonly GlobalStateStoreProvider globalStateStoreProvider;

        public QueryableStoreProvider(IEnumerable<IStateStoreProvider> storeProviders
            /*GlobalStateStoreProvider globalStateStoreProvider*/)
        {
            this.storeProviders = new List<IStateStoreProvider>(storeProviders);
            //this.globalStateStoreProvider = globalStateStoreProvider;
        }

        /// <summary>
        /// Get a composite object wrapping the instances of the <see cref="Processors.IStateStore"/> with the provided
        /// storeName and <see cref="IQueryableStoreType{T}"/>
        /// </summary>
        /// <typeparam name="T">The expected type of the returned store</typeparam>
        /// <param name="storeName">name of the store</param>
        /// <param name="queryableStoreType">accept stores passing <see cref="IQueryableStoreType{T}.Accepts(Processors.IStateStore)"/></param>
        /// <returns>A composite object that wraps the store instances.</returns>
        public T GetStore<T>(string storeName, IQueryableStoreType<T> queryableStoreType) where T : class//, IStateStore
        {
            //IEnumerable<T> globalStore = this.globalStateStoreProvider.stores(storeName, queryableStoreType);
            //if (globalStore.Any())
            //{
            //    return queryableStoreType.Create(new WrappingStoreProvider(new[] { this.globalStateStoreProvider })), storeName);
            //}

            IEnumerable<T> allStores = this.storeProviders
                .SelectMany(store => store.Stores(storeName, queryableStoreType));

            if (!allStores.Any())
            {
                throw new InvalidStateStoreException($"The state store, {storeName}, may have migrated to another instance.");
            }

            return queryableStoreType.Create(new WrappingStoreProvider(this.storeProviders), storeName);
        }
    }
}
