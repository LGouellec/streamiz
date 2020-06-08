using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State.InMemory
{
    public class InMemoryTimestampedWindowStoreSupplier : WindowBytesStoreSupplier
    {
        private string storeName;
        private TimeSpan retention;
        private long size;

        public InMemoryTimestampedWindowStoreSupplier(string storeName, TimeSpan retention, long size)
        {
            this.storeName = storeName;
            this.retention = retention;
            this.size = size;
        }

        public string Name => throw new NotImplementedException();

        public WindowStore<Bytes, byte[]> Get()
        {
            throw new NotImplementedException();
        }
    }
}
