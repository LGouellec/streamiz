using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public class ProcessorSupplier<K, V>
    {
        public IProcessor<K,V> Processor { get; internal set; }
        public StoreBuilder StoreBuilder { get; internal set; }
    }
}