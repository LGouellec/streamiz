using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State.InMemory
{
    public class InMemoryTimestampedWindowStoreSupplier : WindowBytesStoreSupplier
    {
        private readonly TimeSpan retention;

        public InMemoryTimestampedWindowStoreSupplier(string storeName, TimeSpan retention, long size)
        {
            Name = storeName;
            this.retention = retention;
            WindowSize = size;
        }

        public string Name { get; }

        public long WindowSize { get; }

        public WindowStore<Bytes, byte[]> Get()
            => new InMemoryWindowStore(Name, retention, WindowSize);
    }
}
