using Kafka.Streams.Net.Crosscutting;

namespace Kafka.Streams.Net.State.Supplier
{
    public interface KeyValueBytesStoreSupplier : StoreSupplier<KeyValueStore<Bytes, byte[]>>
    {
    }
}
