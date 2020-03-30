using kafka_stream_core.Crosscutting;
using kafka_stream_core.State.Supplier;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State.InMemory
{
    internal class InMemoryKeyValueBytesStoreSupplier : KeyValueBytesStoreSupplier
    {
        public InMemoryKeyValueBytesStoreSupplier(string name)
        {
            this.Name = name;
        }

        public string Name { get; }

        public KeyValueStore<Bytes, byte[]> get() => new InMemoryKeyValueStore(this.Name);
    }
}
