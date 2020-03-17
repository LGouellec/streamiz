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
        private String[] storeNames;

        public TableProcessorNode(string name, ProcessorParameters<K, Change<V>> processorParameters, StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder)
            : this(name, processorParameters, storeBuilder, null)
        {
        }

        public TableProcessorNode(string name, ProcessorParameters<K, Change<V>> processorParameters, StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder, string[] storeNames)
            : base(name)
        {
            this.processorParameters = processorParameters;
            this.storeBuilder = storeBuilder;
            this.storeNames = storeNames != null ? storeNames : new string[0];
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            // TODO : 
            String processorName = processorParameters.ProcessorName;
            builder.addProcessor(processorName, processorParameters.Processor);

            if (storeNames.Length > 0)
            {
                //builder.connectProcessorAndStateStores(processorName, storeNames);
            }

            // TODO: we are enforcing this as a keyvalue store, but it should go beyond any type of stores
            if (storeBuilder != null)
            {
                //builder.addStateStore(storeBuilder, processorName);
            }
        }
    }
}
