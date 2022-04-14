using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.Supplier
{
    /// <summary>
    /// A state store supplier which can create one or more <see cref="IStateStore"/> instances.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IStoreSupplier<out T> 
        where T : IStateStore
    {
        /// <summary>
        /// Return the name of this state store supplier.
        /// This must be a valid Kafka topic name; valid characters are ASCII alphanumerics, '.', '_' and '-'.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Return a new <see cref="IStateStore"/> instance of type <typeparamref name="T"/>.
        /// </summary>
        /// <returns>Return a new <see cref="IStateStore"/> instance of type <typeparamref name="T"/>.</returns>
        T Get();
        
        /// <summary>
        /// Return a String that is used as the scope for metrics recorded by Metered stores.
        /// </summary>
        string MetricsScope { get; }
    }
}
