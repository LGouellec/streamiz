using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using kafka_stream_core.State;
using kafka_stream_core.State.Internal;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using kafka_stream_core.Stream.Internal.Graph;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using kafka_stream_core.Table.Internal.Graph;
using kafka_stream_core.Table.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Table.Internal
{
    internal class KTableImpl<K, V> : AbstractStream<K, V>, KTable<K, V>
    {
        private readonly IProcessorSupplier<K, V> processorSupplier = null;
        private readonly IProcessorSupplier<K, Change<V>> tableProcessorSupplier = null;
        private readonly string queryableStoreName;
        
        public bool SendOldValues { get; private set; }

        public IKTableValueGetterSupplier<K, V> ValueGetterSupplier
        {
            get
            {
                if (processorSupplier != null && processorSupplier is KTableSource<K, V>)
                {
                    KTableSource<K, V> source = (KTableSource<K, V>)processorSupplier;
                    // whenever a source ktable is required for getter, it should be materialized
                    source.Materialize();
                    return new KTableSourceValueGetterSupplier<K,V>(source.QueryableName);
                }
                // TODO : 
                //} else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                //    return ((KStreamAggProcessorSupplier <?, K, S, V >) processorSupplier).view();
                //} else
                else if (tableProcessorSupplier != null && tableProcessorSupplier is IKTableProcessorSupplier<K, V, V>)
                {
                    return ((IKTableProcessorSupplier<K, V, V>)tableProcessorSupplier).View;
                }
                else
                    return null;
            }
        }

        #region Constants

        internal static String FOREACH_NAME = "KTABLE-FOREACH-";

        internal static String SOURCE_NAME = "KTABLE-SOURCE-";

        internal static String STATE_STORE_NAME = "STATE-STORE-";

        internal static String FILTER_NAME = "KTABLE-FILTER-";

        internal static String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

        internal static String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

        internal static String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

        internal static String MERGE_NAME = "KTABLE-MERGE-";

        internal static String SELECT_NAME = "KTABLE-SELECT-";

        internal static String SUPPRESS_NAME = "KTABLE-SUPPRESS-";

        internal static String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

        internal static String TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";

        internal static String SINK_NAME = "KTABLE-SINK-";

        internal static String FK_JOIN = "KTABLE-FK-JOIN-";
        internal static String FK_JOIN_STATE_STORE_NAME = FK_JOIN + "SUBSCRIPTION-STATE-STORE-";
        internal static String SUBSCRIPTION_REGISTRATION = FK_JOIN + "SUBSCRIPTION-REGISTRATION-";
        internal static String SUBSCRIPTION_RESPONSE = FK_JOIN + "SUBSCRIPTION-RESPONSE-";
        internal static String SUBSCRIPTION_PROCESSOR = FK_JOIN + "SUBSCRIPTION-PROCESSOR-";
        internal static String SUBSCRIPTION_RESPONSE_RESOLVER_PROCESSOR = FK_JOIN + "SUBSCRIPTION-RESPONSE-RESOLVER-PROCESSOR-";
        internal static String FK_JOIN_OUTPUT_NAME = FK_JOIN + "OUTPUT-";

        internal static String TOPIC_SUFFIX = "-topic";

        #endregion


        internal KTableImpl(string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, String queryableStoreName, IProcessorSupplier<K, Change<V>> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.tableProcessorSupplier = processorSupplier;
            this.queryableStoreName = queryableStoreName;
        }

        internal KTableImpl(string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, String queryableStoreName, IProcessorSupplier<K, V> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
    : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.processorSupplier = processorSupplier;
            this.queryableStoreName = queryableStoreName;
        }

        #region Privates

        private KTable<K, V> doFilter(Func<K, V, bool> predicate, string named, Materialized<K, V, KeyValueStore<byte[], byte[]>> materializedInternal, bool filterNot)
        {
            ISerDes<K> keySerde;
            ISerDes<V> valueSerde;
            String queryableStoreName;
            StoreBuilder<kafka_stream_core.State.TimestampedKeyValueStore<K, V>> storeBuilder;

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at all since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    builder.newStoreName(FILTER_NAME);
                }
                // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
                // we preserve the key following the order of 1) materialized, 2) parent
                keySerde = materializedInternal.KeySerdes != null ? materializedInternal.KeySerdes : this.keySerdes;
                // we preserve the value following the order of 1) materialized, 2) parent
                valueSerde = materializedInternal.ValueSerdes != null ? materializedInternal.ValueSerdes : this.valueSerdes;

                queryableStoreName = materializedInternal.QueryableStoreName;
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K,V>(materializedInternal)).materialize() : null;
            }
            else
            {
                keySerde = this.keySerdes;
                valueSerde = this.valueSerdes;
                queryableStoreName = null;
                storeBuilder = null;
            }

            String name = this.builder.newProcessorName(FILTER_NAME);

            IProcessorSupplier<K, Change<V>> processorSupplier = new KTableFilter<K, V>(this, predicate, filterNot, queryableStoreName);

            var processorParameters = new TableProcessorParameters<K, V>(processorSupplier, name);

            var tableNode = new TableProcessorNode<K, V>(
               name,
               processorParameters,
               storeBuilder
           );

            builder.addGraphNode(this.node, tableNode);

            return new KTableImpl<K, V>(name,
                                    keySerde,
                                    valueSerde,
                                    this.setSourceNodes,
                                    queryableStoreName,
                                    processorSupplier,
                                    tableNode,
                                    builder);
        }

        #endregion

        #region KTable Impl

        #region Filter

        public KTable<K, V> filter(Func<K, V, bool> predicate) => doFilter(predicate, null, null, false);

        public KTable<K, V> filter(Func<K, V, bool> predicate, string named) => doFilter(predicate, named, null, false);

        public KTable<K, V> filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized)
            => doFilter(predicate, null, materialized, false);

        public KTable<K, V> filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized, string named)
            => doFilter(predicate, named, materialized, false);

        public KTable<K, V> filterNot(Func<K, V, bool> predicate) => doFilter(predicate, null, null, true);

        public KTable<K, V> filterNot(Func<K, V, bool> predicate, string name) => doFilter(predicate, name, null, true);

        public KTable<K, V> filterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized) 
            => doFilter(predicate, null, materialized, true);

        public KTable<K, V> filterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized, string named)
            => doFilter(predicate, named, materialized, true);

        #endregion

        #region ToStream

        public KStream<K, V> toStream() => this.toStream(null);

        public KStream<K, V> toStream(string named)
        {
            string name = this.builder.newProcessorName(TOSTREAM_NAME);

            var p = new ValueMapperWithKey<K, Change<V>, V>((k, v) => v.NewValue);
            IProcessorSupplier<K, Change<V>> processorMapValues = new KStreamMapValues<K, Change<V>, V>(p);
            ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<K, Change<V>>(processorMapValues, name);

            ProcessorGraphNode<K, Change<V>> toStreamNode = new ProcessorGraphNode<K, Change<V>>(name, processorParameters);

            builder.addGraphNode(this.node, toStreamNode);

            // we can inherit parent key and value serde
            return new KStreamImpl<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, toStreamNode, builder);
        }

        #endregion

        #endregion

        public void EnableSendingOldValues()
        {
            if (!SendOldValues)
            {
                if (processorSupplier != null && processorSupplier is KTableSource<K, V>)
                {
                    KTableSource<K, V> source = (KTableSource<K, V>)processorSupplier;
                    source.enableSendingOldValues();
                }
                // TODO : 
                //} else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                //    ((KStreamAggProcessorSupplier <?, K, S, V >) processorSupplier).enableSendingOldValues();
                //} else
                else if(tableProcessorSupplier != null && tableProcessorSupplier is IKTableProcessorSupplier)
                {
                    (tableProcessorSupplier as IKTableProcessorSupplier).enableSendingOldValues();
                }
                SendOldValues = true;
            }
        }
    }
}