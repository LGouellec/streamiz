using kafka_stream_core.Crosscutting;

namespace kafka_stream_core.State.Supplier
{
    public interface KeyValueBytesStoreSupplier : StoreSupplier<KeyValueStore<Bytes, byte[]>>
    {
    }
}
