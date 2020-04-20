using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Supplier
{
    /// <summary>
    /// A store supplier that can be used to create one or more <see cref="KeyValueStore{K, V}"/> instances of type &lt;Bytes, byte[]&gt;.
    /// For any stores implementing the KeyValueStore&lt;Byte, byte[]&gt; interface, null value bytes are considered as "not exist". This means:
    /// 1. Null value bytes in put operations should be treated as delete.
    /// 2. If the key does not exist, get operations should return null value bytes.
    /// </summary>
    public interface KeyValueBytesStoreSupplier : StoreSupplier<KeyValueStore<Bytes, byte[]>>
    {
    }
}
