using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public class TransformerBuilder
    {
        public static TransformerBuilder<K, V, K1, V1> New<K, V, K1, V1>()
            => new();
    }
    
    public class TransformerBuilder<K, V, K1, V1>
    {
        private TransformerSupplier<K, V, K1, V1> transformerSupplier = new();

        public TransformerBuilder<K, V, K1, V1> Transformer(ITransformer<K, V, K1, V1> transformer)
        {
            transformerSupplier.Transformer = transformer;
            return this;
        }
        
        public TransformerBuilder<K, V, K1, V1> Transformer(Func<Record<K, V>, Record<K1, V1>> transformer)
        {
            transformerSupplier.Transformer = new WrappedTransformer<K, V, K1, V1>(transformer);
            return this;
        }

        public TransformerBuilder<K, V, K1, V1> StateStore(StoreBuilder storeBuilder)
        {
            transformerSupplier.StoreBuilder = storeBuilder;
            return this;
        }

        public TransformerSupplier<K, V, K1, V1> Build()
        {
            TransformerSupplier<K, V, K1, V1> transformer = transformerSupplier;
            transformerSupplier = new();
            return transformer;
        }
    }
}