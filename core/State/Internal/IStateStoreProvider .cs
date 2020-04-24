using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Provides access to <see cref="Processors.IStateStore"/>s that have been created
    /// as part of the <see cref="Stream.Internal.ProcessorTopology"/>.
    /// To get access to custom stores developers should implement <see cref="IQueryableStoreType{T}"/>.
    /// </summary>
    // /// <see cref="IQueryableStoreTypes"/>
    public interface IStateStoreProvider
    {
        /// <summary>
        /// Find instances of <see cref="Processors.IStateStore"/> that are accepted by <see cref="IQueryableStoreType{T}.Accepts(Processors.IStateStore)"/> and
        /// have the provided storeName.
        /// </summary>
        /// <typeparam name="T">The type of the Store</typeparam>
        /// <param name="storeName">name of the store</param>
        /// <param name="queryableStoreType">filter stores based on this queryableStoreType</param>
        /// <returns>List of the instances of the store in this topology. Empty List if not found</returns>
        IEnumerable<T> Stores<T>(string storeName, IQueryableStoreType<T> queryableStoreType) where T : class;//, IStateStore;
    }
}
