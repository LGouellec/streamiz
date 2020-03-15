using kafka_stream_core.Processors;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

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
