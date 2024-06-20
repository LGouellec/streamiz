using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A rocksdb key/value store supplier used to create <see cref="RocksDbKeyValueStore"/>.
    /// </summary>
    public class RocksDbKeyValueBytesStoreSupplier : IKeyValueBytesStoreSupplier
    {
        /// <summary>
        /// Constructor with state store name
        /// </summary>
        /// <param name="name">state store name</param>
        public RocksDbKeyValueBytesStoreSupplier(string name)
        {
            Name = name;
        }

        /// <summary>
        /// Return a String that is used as the scope for metrics recorded by Metered stores.
        /// </summary>
        public string MetricsScope => "rocksdb";
        
        /// <summary>
        /// State store name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Build the rocksdb state store.
        /// </summary>
        /// <returns><see cref="RocksDbKeyValueStore"/> state store</returns>
        public IKeyValueStore<Bytes, byte[]> Get() =>
            new RocksDbKeyValueStore(Name);
    }
}
