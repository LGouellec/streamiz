using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    public class RocksDbKeyValueBytesStoreSupplier : KeyValueBytesStoreSupplier
    {
        public RocksDbKeyValueBytesStoreSupplier(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public IKeyValueStore<Bytes, byte[]> Get() =>
            new RocksDbKeyValueStore(Name);
    }
}
