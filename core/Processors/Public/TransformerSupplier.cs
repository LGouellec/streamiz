using System;
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
        private ITransformer<K, V, K1, V1> innerTransformer;
        
        /// <summary>
        /// Get a copy of your transformer
        /// </summary>
        public ITransformer<K,V,K1,V1> Transformer {
            get
            {
                if (innerTransformer == null)
                    return null;
                if (innerTransformer is ICloneableProcessor cloneableProcessor)
                    return (ITransformer<K,V,K1,V1>)cloneableProcessor.Clone();
                return (ITransformer<K,V,K1,V1>)Activator.CreateInstance(innerTransformer.GetType());
            }
            internal set => innerTransformer = value;
        }
        
        /// <summary>
        /// Current state store builder (may be null)
        /// </summary>
        public IStoreBuilder StoreBuilder { get; internal set; }
    }
}