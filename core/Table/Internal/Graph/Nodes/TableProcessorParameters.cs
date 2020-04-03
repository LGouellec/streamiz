using kafka_stream_core.Processors;
using kafka_stream_core.Stream.Internal.Graph.Nodes;

namespace kafka_stream_core.Table.Internal.Graph.Nodes
{
    internal class TableProcessorParameters<K, V> : ProcessorParameters<K, Change<V>>
    {
        public TableProcessorParameters(IProcessorSupplier<K, Change<V>> processorSupplier, string processorName) 
            : base(processorSupplier, processorName)
        {
        }
    }
}
