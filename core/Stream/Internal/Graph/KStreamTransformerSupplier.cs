using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamTransformerSupplier<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly TransformerSupplier<K, V, K1, V1> transformerSupplier;
        private readonly bool changeKey;

        public KStreamTransformerSupplier(TransformerSupplier<K, V, K1, V1> transformerSupplier, bool changeKey)
        {
            this.transformerSupplier = transformerSupplier;
            this.changeKey = changeKey;
        }

        public Processors.IProcessor<K, V> Get()
            => new KStreamTransformer<K, V, K1, V1>(transformerSupplier, changeKey);
    }
}