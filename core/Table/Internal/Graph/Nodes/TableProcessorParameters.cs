using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Table.Internal.Graph.Nodes
{
    internal class TableProcessorParameters<K, V> : ProcessorParameters<K, Change<V>>
    {
        public TableProcessorParameters(IProcessorSupplier<K, Change<V>> processorSupplier, string processorName) 
            : base(processorSupplier, processorName)
        {
        }
    }
}
