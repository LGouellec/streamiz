using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// Create a new <see cref="TransformerBuilder{K,V,K1,V1}"/> instance
    /// </summary>
    public class TransformerBuilder
    {
        /// <summary>
        /// Helper create method
        /// </summary>
        /// <typeparam name="K">type of the key</typeparam>
        /// <typeparam name="V">type of the value</typeparam>
        /// <typeparam name="K1">new type of the key</typeparam>
        /// <typeparam name="V1">new type of the value</typeparam>
        /// <returns>return a new <see cref="TransformerBuilder{K,V,K1,V1}"/> instance</returns>
        public static TransformerBuilder<K, V, K1, V1> New<K, V, K1, V1>()
            => new();
    }
    
    /// <summary>
    /// Builder's <see cref="TransformerSupplier{K,V,K1,V1}"/>
    /// </summary>
    /// <typeparam name="K">type of the key</typeparam>
    /// <typeparam name="V">type of the value</typeparam>
    /// <typeparam name="K1">new type of the key</typeparam>
    /// <typeparam name="V1">new type of the value</typeparam>
    public class TransformerBuilder<K, V, K1, V1>
    {
        private TransformerSupplier<K, V, K1, V1> transformerSupplier = new();

        /// <summary>
        /// Set the transformer
        /// </summary>
        /// <param name="transformer"></param>
        /// <returns></returns>
        public TransformerBuilder<K, V, K1, V1> Transformer(ITransformer<K, V, K1, V1> transformer)
        {
            transformerSupplier.Transformer = transformer;
            return this;
        }
        
        /// <summary>
        /// Set the transformer
        /// </summary>
        /// <param name="transformer"></param>
        /// <returns></returns>
        public TransformerBuilder<K, V, K1, V1> Transformer(Func<Record<K, V>, Record<K1, V1>> transformer)
        {
            transformerSupplier.Transformer = new WrappedTransformer<K, V, K1, V1>(transformer);
            return this;
        }

        /// <summary>
        /// Set the state store
        /// </summary>
        /// <param name="storeBuilder"></param>
        /// <returns></returns>
        public TransformerBuilder<K, V, K1, V1> StateStore(IStoreBuilder storeBuilder)
        {
            transformerSupplier.StoreBuilder = storeBuilder;
            return this;
        }

        /// <summary>
        /// Build this <see cref="TransformerSupplier{K,V,K1,V1}"/>
        /// </summary>
        /// <returns></returns>
        public TransformerSupplier<K, V, K1, V1> Build()
        {
            TransformerSupplier<K, V, K1, V1> transformer = transformerSupplier;
            transformerSupplier = new();
            return transformer;
        }
    }
}