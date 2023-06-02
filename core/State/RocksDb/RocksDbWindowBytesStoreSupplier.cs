using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.RocksDb.Internal;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    /// <summary>
    /// A rocksdb key/value store supplier used to create <see cref="RocksDbWindowStore"/>.
    /// </summary>
    public class RocksDbWindowBytesStoreSupplier : IWindowBytesStoreSupplier
    {
        private readonly long segmentInterval;
        private readonly bool retainDuplicates;

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
            this.segmentInterval = segmentInterval;
            RetainDuplicates = retainDuplicates;
            WindowSize = size;
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
        /// Build the rocksdb state store.
        /// </summary>
        /// <returns><see cref="RocksDbWindowStore"/> state store</returns>
        public IWindowStore<Bytes, byte[]> Get()
        {
            return new RocksDbWindowStore(
                new RocksDbSegmentedBytesStore(
                    Name,
                    Retention,
                    segmentInterval,
                    new RocksDbWindowKeySchema()),
                WindowSize.HasValue ? WindowSize.Value : (long)TimeSpan.FromMinutes(1).TotalMilliseconds,
                RetainDuplicates);
        }
    }
}
