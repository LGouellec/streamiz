using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Supplier
{
    public interface KeyValueBytesStoreSupplier : StoreSupplier<KeyValueStore<Bytes, byte[]>>
    {
    }
}
