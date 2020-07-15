using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamJoinWindow<K, V> : IProcessorSupplier<K, V>
    {
        private readonly string storeName;

        public KStreamJoinWindow(string storeName)
        {
            this.storeName = storeName;
        }

        public IProcessor<K, V> Get() => new KStreamJoinWindowProcessor<K, V>(storeName);
    }
}
