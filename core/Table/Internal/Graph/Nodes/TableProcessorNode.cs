using kafka_stream_core.Processors.Internal;
using kafka_stream_core.State;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal.Graph.Nodes
{
    internal class TableProcessorNode<K, V> : StreamGraphNode
    {
        private ProcessorParameters<K, Change<V>> processorParameters;
        private StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder;

        public TableProcessorNode(string name, ProcessorParameters<K, Change<V>> processorParameters, StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder)
            : base(name)
        {
            this.processorParameters = processorParameters;
            this.storeBuilder = storeBuilder;
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            // TODO : 
            throw new NotImplementedException();
        }
    }
}
