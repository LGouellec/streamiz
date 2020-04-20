using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State.InMemory
{
    /// <summary>
    /// A store supplier that can be used to create one or more <see cref="InMemoryKeyValueStore"/> instances.
    /// </summary>
    public class InMemoryKeyValueBytesStoreSupplier : KeyValueBytesStoreSupplier
    {
        /// <summary>
        /// Constructor with the name of this state store supplier.
        /// </summary>
        /// <param name="name">Name of this state store supplier. This must be a valid Kafka topic name; valid characters are ASCII alphanumerics, '.', '_' and '-'.</param>
        public InMemoryKeyValueBytesStoreSupplier(string name)
        {
            this.Name = name;
        }

        /// <summary>
        /// Name of this state store supplier. This must be a valid Kafka topic name; valid characters are ASCII alphanumerics, '.', '_' and '-'.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Return a new <see cref="IStateStore"/> instance.
        /// </summary>
        /// <returns>Return a new <see cref="InMemoryKeyValueStore"/>instance.</returns>
        public IKeyValueStore<Bytes, byte[]> Get() => new InMemoryKeyValueStore(this.Name);
    }
}
