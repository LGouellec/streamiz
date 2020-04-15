using Kafka.Streams.Net.Processors;
using Kafka.Streams.Net.Stream.Internal.Graph.Nodes;

namespace Kafka.Streams.Net.Table.Internal.Graph.Nodes
{
    internal class TableProcessorParameters<K, V> : ProcessorParameters<K, Change<V>>
    {
        public TableProcessorParameters(IProcessorSupplier<K, Change<V>> processorSupplier, string processorName) 
            : base(processorSupplier, processorName)
        {
        }
    }
}
