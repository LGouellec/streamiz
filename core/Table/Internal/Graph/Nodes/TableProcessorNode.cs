using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Table.Internal.Graph.Nodes
{
    internal abstract class TableProcessorNode : StreamGraphNode
    {
        internal TableProcessorNode(string streamGraphNode) 
            : base(streamGraphNode)
        {
        }
    }

    internal class TableProcessorNode<K, V, KS, VS> : TableProcessorNode
    {
        private ProcessorParameters<K, Change<V>> processorParameters;
        private StoreBuilder<TimestampedKeyValueStore<KS, VS>> storeBuilder;
        private String[] storeNames;

        public TableProcessorNode(string name, ProcessorParameters<K, Change<V>> processorParameters, StoreBuilder<TimestampedKeyValueStore<KS, VS>> storeBuilder)
            : this(name, processorParameters, storeBuilder, null)
        {
        }

        public TableProcessorNode(string name, ProcessorParameters<K, Change<V>> processorParameters, StoreBuilder<TimestampedKeyValueStore<KS, VS>> storeBuilder, string[] storeNames)
            : base(name)
        {
            this.processorParameters = processorParameters;
            this.storeBuilder = storeBuilder;
            this.storeNames = storeNames != null ? storeNames : new string[0];
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            // TODO : 
            String processorName = processorParameters.ProcessorName;
            builder.AddProcessor(processorName, processorParameters.Processor);

            if (storeNames.Length > 0)
            {
                //builder.connectProcessorAndStateStores(processorName, storeNames);
            }

            // TODO: we are enforcing this as a keyvalue store, but it should go beyond any type of stores
            if (storeBuilder != null)
            {
                builder.AddStateStore(storeBuilder, processorName);
            }
        }
    }
}
