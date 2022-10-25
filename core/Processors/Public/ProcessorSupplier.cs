using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// Materialize the processor parameters for <see cref="IKStream{K,V}.Process"/>
    /// </summary>
    /// <typeparam name="K">type of the key</typeparam>
    /// <typeparam name="V">type of the value</typeparam>
    public class ProcessorSupplier<K, V>
    {
        /// <summary>
        /// Current processor
        /// </summary>
        public IProcessor<K,V> Processor { get; internal set; }
        
        /// <summary>
        /// Current state store builder (may be null)
        /// </summary>
        public StoreBuilder StoreBuilder { get; internal set; }
    }
}