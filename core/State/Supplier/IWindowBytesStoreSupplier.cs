using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Supplier
{
    /// <summary>
    /// A store supplier that can be used to create one or more <see cref="IWindowStore{K, V}"/> instances of type &lt;Bytes, byte[]&gt;.
    /// 
    /// For any stores implementing the <see cref="IWindowStore{K, V}"/> interface, null value bytes are considered as "not exist". 
    /// This means:
    /// 1. Null value bytes in put operations should be treated as delete.
    /// 2. Null value bytes should never be returned in range query results.
    /// </summary>
    public interface IWindowBytesStoreSupplier : IStoreSupplier<IWindowStore<Bytes, byte[]>>
    {
        /// <summary>
        /// The size of the windows (in milliseconds) any store created from this supplier is creating.
        /// Can be null
        /// </summary>
        public long? WindowSize { get; set; }

        /// <summary>
        /// The time period for which the <see cref="IWindowStore{K, V}"/> will retain historic data.
        /// </summary>
        public long Retention { get; set; }

        /// <summary>
        /// Whether or not this store is retaining duplicate keys.
        /// Usually only true if the store is being used for joins.
        /// Note this should return false if caching is enabled.
        /// </summary>
        public bool RetainDuplicates { get; set; }
        
        /// <summary>
        /// The size of the segments (in milliseconds) the store has.
        /// If your store is segmented then this should be the size of segments in the underlying store.
        /// It is also used to reduce the amount of data that is scanned when caching is enabled.
        /// </summary>
        public long SegmentInterval { get; set; }
    }
}
