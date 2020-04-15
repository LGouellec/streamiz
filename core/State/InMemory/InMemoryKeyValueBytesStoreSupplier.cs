using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.State.Supplier;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State.InMemory
{
    internal class InMemoryKeyValueBytesStoreSupplier : KeyValueBytesStoreSupplier
    {
        public InMemoryKeyValueBytesStoreSupplier(string name)
        {
            this.Name = name;
        }

        public string Name { get; }

        public KeyValueStore<Bytes, byte[]> Get() => new InMemoryKeyValueStore(this.Name);
    }
}
