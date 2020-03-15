using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using kafka_stream_core.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal.Graph.Nodes
{
    internal class TableSourceNode<K, V, S> : StreamSourceNode<K, V>
        where S : StateStore
    {
        private readonly Materialized<K, V, S> materialized;
        private readonly ProcessorParameters<K, V> processorParameters;
        private readonly String sourceName;
        private readonly bool isGlobalKTable;
        private bool shouldReuseSourceTopicForChangelog = false;

        public TableSourceNode(string topicName, string streamGraphNode,
                string sourceName, Consumed<K, V> consumed,
                Materialized<K, V, S> materialized, ProcessorParameters<K, V> processorParameters, bool isGlobalKTable = false)
            : base(topicName, streamGraphNode, consumed)
        {
            this.materialized = materialized;
            this.processorParameters = processorParameters;
            this.sourceName = sourceName;
            this.isGlobalKTable = isGlobalKTable;
        }

        public void reuseSourceTopicForChangeLog(bool shouldReuseSourceTopicForChangelog)
        {
            this.shouldReuseSourceTopicForChangelog = shouldReuseSourceTopicForChangelog;
        }

        public override string ToString()
        {
            return "TableSourceNode{" +
                   "materializedInternal=" + materialized +
                   ", processorParameters=" + processorParameters +
                   ", sourceName='" + sourceName + '\'' +
                   ", isGlobalKTable=" + isGlobalKTable +
                   "} " + base.ToString();
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
            // TODO: we assume source KTables can only be timestamped-key-value stores for now.
            // should be expanded for other types of stores as well.
            //StoreBuilder<TimestampedKeyValueStore< K, V >> storeBuilder = new TimestampedKeyValueStoreMaterializer<>((MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>>)materializedInternal).materialize();

            if (isGlobalKTable)
            {
                // TODO : 
                //topologyBuilder.addGlobalStore(storeBuilder,
                //                               sourceName,
                //                               consumedInternal().timestampExtractor(),
                //                               consumedInternal().keyDeserializer(),
                //                               consumedInternal().valueDeserializer(),
                //                               topicName,
                //                               processorParameters.processorName(),
                //                               processorParameters.processorSupplier());
            }
            else
            {
                builder.addSourceOperator<K, V>(this.topicName, sourceName, consumed);
                builder.addProcessor<K, V>(processorParameters.ProcessorName, processorParameters.Processor);

                //// only add state store if the source KTable should be materialized
                KTableSource<K, V> ktableSource = (KTableSource<K, V>)processorParameters.Processor;
                if (ktableSource.QueryableName != null)
                {
                    // TODO :
                    //builder.addStateStore(storeBuilder, nodeName());

                    //if (shouldReuseSourceTopicForChangelog)
                    //{
                    //    storeBuilder.withLoggingDisabled();
                    //    topologyBuilder.connectSourceStoreAndTopic(storeBuilder.name(), topicName);
                    //}
                }
            }
        }
    }
}
