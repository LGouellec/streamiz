using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using System;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A rocksdb key/value store supplier used to create <see cref="RocksDbWindowStore"/>.
    /// </summary>
    public class RocksDbWindowBytesStoreSupplier : IWindowBytesStoreSupplier
    {
        /// <summary>
        /// Constructor with some arguments.
        /// </summary>
        /// <param name="storeName">state store name</param>
        /// <param name="retention">retention of windowing store</param>
        /// <param name="segmentInterval">segment interval</param>
        /// <param name="size">window size</param>
        /// <param name="retainDuplicates">whether or not to retain duplicates</param>
        public RocksDbWindowBytesStoreSupplier(
            string storeName,
            TimeSpan retention,
            long segmentInterval,
            long? size,
            bool retainDuplicates)
        {
            Name = storeName;
            Retention = (long)retention.TotalMilliseconds;
            RetainDuplicates = retainDuplicates;
            WindowSize = size;
            SegmentInterval = segmentInterval;
        }
        
        /// <summary>
        /// Return a String that is used as the scope for metrics recorded by Metered stores.
        /// </summary>
        public string MetricsScope => "rocksdb-window";
        
        /// <summary>
        /// Window size of the state store
        /// </summary>
        public long? WindowSize { get; set; }

        /// <summary>
        /// Retention of the state store
        /// </summary>
        public long Retention { get; set; }
        
        /// <summary>
        /// Whether or not this store is retaining duplicate keys.
        /// Usually only true if the store is being used for joins.
        /// Note this should return false if caching is enabled.
        /// </summary>
        public bool RetainDuplicates { get; set; }

        /// <summary>
        /// State store name
        /// </summary>
        public string Name { get; set; }
        
        /// <summary>
        /// The size of the segments (in milliseconds) the store has.
        /// If your store is segmented then this should be the size of segments in the underlying store.
        /// It is also used to reduce the amount of data that is scanned when caching is enabled.
        /// </summary>
        public long SegmentInterval { get; set; }

        /// <summary>
        /// Build the rocksdb state store.
        /// </summary>
        /// <returns><see cref="RocksDbWindowStore"/> state store</returns>
        public IWindowStore<Bytes, byte[]> Get()
        {
            return new RocksDbWindowStore(
                new RocksDbSegmentedBytesStore(
                    Name,
                    Retention,
                    SegmentInterval,
                    new WindowKeySchema()),
                WindowSize.HasValue ? WindowSize.Value : (long)TimeSpan.FromMinutes(1).TotalMilliseconds,
                RetainDuplicates);
        }
    }
}
