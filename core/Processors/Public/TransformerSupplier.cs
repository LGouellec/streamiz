using System.Diagnostics;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// Materialize the processor parameters for <see cref="IKStream{K,V}.Transform{K1,V1}"/>
    /// </summary>
    /// <typeparam name="K">type of the key</typeparam>
    /// <typeparam name="V">type of the value</typeparam>
    /// <typeparam name="K1">new type of the key</typeparam>
    /// <typeparam name="V1">new type of the value</typeparam>
    public class TransformerSupplier<K, V, K1, V1>
    {
        /// <summary>
        /// Current transformer
        /// </summary>
        public ITransformer<K,V,K1,V1> Transformer { get; internal set; }
        
        /// <summary>
        /// Current state store builder (may be null)
        /// </summary>
        public IStoreBuilder StoreBuilder { get; internal set; }
    }
}