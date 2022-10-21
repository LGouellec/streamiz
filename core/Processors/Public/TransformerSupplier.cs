using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public class TransformerSupplier<K, V, K1, V1>
    {
        public ITransformer<K,V,K1,V1> Transformer { get; internal set; }
        public StoreBuilder StoreBuilder { get; internal set; }
    }
}