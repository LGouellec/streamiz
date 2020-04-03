using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.State;
using kafka_stream_core.State.Internal;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using kafka_stream_core.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal.Graph.Nodes
{
    internal interface ITableSourceNode
    {
        string SourceName { get; }
        string NodeName { get; }
    }

    internal class TableSourceNode<K, V, S> : StreamSourceNode<K, V>, ITableSourceNode
        where S : IStateStore
    {
        private readonly Materialized<K, V, S> materialized;
        private readonly ProcessorParameters<K, V> processorParameters;
        private readonly String sourceName;
        private readonly bool isGlobalKTable;
        private bool shouldReuseSourceTopicForChangelog = false;

        public TableSourceNode(string topicName, string streamGraphNode,
                string sourceName, ConsumedInternal<K, V> consumed,
                Materialized<K, V, S> materialized, ProcessorParameters<K, V> processorParameters, bool isGlobalKTable = false)
            : base(topicName, streamGraphNode, consumed)
        {
            this.materialized = materialized;
            this.processorParameters = processorParameters;
            this.sourceName = sourceName;
            this.isGlobalKTable = isGlobalKTable;
        }

        public string SourceName => sourceName;

        public string NodeName => this.streamGraphNode;

        public void ReuseSourceTopicForChangeLog(bool shouldReuseSourceTopicForChangelog)
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

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            // TODO: we assume source KTables can only be timestamped-key-value stores for now.
            // should be expanded for other types of stores as well.
            StoreBuilder<State.TimestampedKeyValueStore<K, V>> storeBuilder = new TimestampedKeyValueStoreMaterializer<K, V>(materialized as Materialized<K, V, KeyValueStore<Bytes, byte[]>>).Materialize();

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
                builder.AddSourceOperator<K, V>(this.topicName, sourceName, consumed);
                builder.AddProcessor<K, V>(processorParameters.ProcessorName, processorParameters.Processor);

                //// only add state store if the source KTable should be materialized
                KTableSource<K, V> ktableSource = (KTableSource<K, V>)processorParameters.Processor;
                if (ktableSource.QueryableName != null)
                {
                    builder.AddStateStore(storeBuilder, this.streamGraphNode);

                    // TODO :

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
