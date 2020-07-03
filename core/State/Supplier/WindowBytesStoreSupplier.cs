using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Supplier
{
    /// <summary>
    /// A store supplier that can be used to create one or more <see cref="WindowStore{K, V}"/> instances of type &lt;Bytes, byte[]&gt;.
    /// 
    /// For any stores implementing the <see cref="WindowStore{K, V}"/> interface, null value bytes are considered as "not exist". 
    /// This means:
    /// 1. Null value bytes in put operations should be treated as delete.
    /// 2. Null value bytes should never be returned in range query results.
    /// </summary>
    public interface WindowBytesStoreSupplier : StoreSupplier<WindowStore<Bytes, byte[]>>
    {
        /// <summary>
        /// The size of the windows (in milliseconds) any store created from this supplier is creating.
        /// Can be null
        /// </summary>
        public long? WindowSize { get; set; }
    }
}
