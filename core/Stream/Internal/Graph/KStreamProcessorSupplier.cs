using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamProcessorSupplier<K, V> : IProcessorSupplier<K, V>
    {
        private readonly ProcessorSupplier<K, V> processorSupplier;

        public KStreamProcessorSupplier(ProcessorSupplier<K, V> processorSupplier)
        {
            this.processorSupplier = processorSupplier;
        }

        public Processors.IProcessor<K, V> Get()
            => new KStreamProcessor<K, V>(processorSupplier);
    }
}