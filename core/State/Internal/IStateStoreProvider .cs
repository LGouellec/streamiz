using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Provides access to <see cref="Processors.IStateStore"/>s that have been created
    /// as part of the <see cref="Stream.Internal.ProcessorTopology"/>.
    /// To get access to custom stores developers should implement <see cref="IQueryableStoreType{T}"/>.
    /// </summary>
    /// <see cref="QueryableStoreTypes"/>
    /// <typeparam name="T">The type of the store to be provided</typeparam>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public interface IStateStoreProvider<T, K, V> 
        where T : class
    {
        /// <summary>
        /// Find instances of <see cref="Processors.IStateStore"/> that are accepted by <see cref="IQueryableStoreType{T, K, V}.Accepts(IStateStore)"/> and
        /// have the provided storeName.
        /// </summary>
        /// <param name="storeName">name of the store</param>
        /// <param name="queryableStoreType">filter stores based on this queryableStoreType</param>
        /// <returns>List of the instances of the store in this topology. Empty List if not found</returns>
        IEnumerable<T> Stores(string storeName, IQueryableStoreType<T, K, V> queryableStoreType);
    }
}
