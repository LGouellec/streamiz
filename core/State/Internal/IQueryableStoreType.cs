using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Used to enable querying of custom <see cref="IStateStore"/> types via the <see cref="KafkaStream"/> API.
    /// </summary>
    /// <typeparam name="T">The store type</typeparam>
    /// <seealso cref="QueryableStoreTypes"/>
    public interface IQueryableStoreType<T> where T : class
    {
        /// <summary>
        /// Called when searching for <see cref="IStateStore"/>s to see if they
        /// match the type expected by implementors of this interface.
        /// </summary>
        /// <param name="stateStore">The stateStore</param>
        /// <returns>true if it is a match</returns>
        bool Accepts(IStateStore stateStore);

        /// <summary>
        /// Create an instance of {@code T} (usually a facade) that developers can use
        /// to query the underlying <see cref="IStateStore"/>s.
        /// </summary>
        /// <param name="storeProvider">provides access to all the underlying <see cref="IStateStore"/> instances</param>
        /// <param name="storeName">The name of the Store</param>
        /// <returns>a read-only interface over a <see cref="IStateStore"/></returns>
        T Create(IStateStoreProvider<T> storeProvider, string storeName);
    }
}
