using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State.InMemory
{
    /// <summary>
    /// A store supplier that can be used to create one or more <see cref="InMemoryWindowStore"/> instances.
    /// </summary>
    public class InMemoryWindowStoreSupplier : WindowBytesStoreSupplier
    {
        private readonly TimeSpan retention;

        /// <summary>
        /// Constructor 
        /// </summary>
        /// <param name="storeName">Name of store</param>
        /// <param name="retention">Retention period of data</param>
        /// <param name="size">Size of window</param>
        public InMemoryWindowStoreSupplier(string storeName, TimeSpan retention, long? size)
        {
            Name = storeName;
            this.retention = retention;
            WindowSize = size;
        }

        /// <summary>
        /// Name of state store
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Window size of state store
        /// </summary>
        public long? WindowSize { get; set; }

        /// <summary>
        /// Retention period of state store
        /// </summary>
        public long Retention => (long)retention.TotalMilliseconds;

        /// <summary>
        /// Return a new <see cref="WindowStore{K, V}"/> instance.
        /// </summary>
        /// <returns>Return a new <see cref="WindowStore{K, V}"/> instance.</returns>
        public WindowStore<Bytes, byte[]> Get()
            => new InMemoryWindowStore(Name, retention, WindowSize.Value);
    }
}
