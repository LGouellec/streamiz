using kafka_stream_core.Crosscutting;
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
    internal class KTableImpl<K, S, V> : AbstractStream<K, V>, KTable<K, V>, KTableGetter<K, V>
    {
        private readonly IProcessorSupplier<K, S> processorSupplier = null;
        private readonly IProcessorSupplier<K, Change<S>> tableProcessorSupplier = null;
        private readonly string queryableStoreName;
        
        public bool SendOldValues { get; private set; }

        public IKTableValueGetterSupplier<K, V> ValueGetterSupplier
        {
            get
            {
                if (processorSupplier != null && processorSupplier is KTableSource<K, V>)
                {
                    var source = (KTableSource<K, V>)processorSupplier;
                    // whenever a source ktable is required for getter, it should be materialized
                    source.Materialize();
                    return new KTableSourceValueGetterSupplier<K, V>(source.QueryableName);
                }
                // TODO : 
                //} else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                //    return ((KStreamAggProcessorSupplier <?, K, S, V >) processorSupplier).view();
                //} else
                else if (tableProcessorSupplier != null && tableProcessorSupplier is IKTableProcessorSupplier<K, S, V>)
                {
                    return ((IKTableProcessorSupplier<K, S, V>)tableProcessorSupplier).View;
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

        internal KTableImpl(string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, String queryableStoreName, IProcessorSupplier<K, Change<S>> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.tableProcessorSupplier = processorSupplier;
            this.queryableStoreName = queryableStoreName;
        }

        internal KTableImpl(string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, String queryableStoreName, IProcessorSupplier<K, S> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.processorSupplier = processorSupplier;
            this.queryableStoreName = queryableStoreName;
        }

        #region KTable Impl

        #region Filter

        public KTable<K, V> filter(Func<K, V, bool> predicate) => doFilter(predicate, null, null, false);

        public KTable<K, V> filter(Func<K, V, bool> predicate, string named) => doFilter(predicate, named, null, false);

        public KTable<K, V> filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized)
            => doFilter(predicate, null, materialized, false);

        public KTable<K, V> filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named)
            => doFilter(predicate, named, materialized, false);

        public KTable<K, V> filterNot(Func<K, V, bool> predicate) => doFilter(predicate, null, null, true);

        public KTable<K, V> filterNot(Func<K, V, bool> predicate, string name) => doFilter(predicate, name, null, true);

        public KTable<K, V> filterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) 
            => doFilter(predicate, null, materialized, true);

        public KTable<K, V> filterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named)
            => doFilter(predicate, named, materialized, true);

        #endregion

        #region ToStream

        public KStream<KR, V> toStream<KR>(Func<K, V, KR> mapper)
            => this.toStream(mapper, string.Empty);

        public KStream<KR, V> toStream<KR>(Func<K, V, KR> mapper, string named)
            => this.toStream(new WrappedKeyValueMapper<K, V, KR>(mapper), named);

        public KStream<KR, V> toStream<KR>(IKeyValueMapper<K, V, KR> mapper)
            => this.toStream(mapper, string.Empty);

        public KStream<KR, V> toStream<KR>(IKeyValueMapper<K, V, KR> mapper, string named)
        {
            return toStream().selectKey(mapper, named);
        }

        public KStream<K, V> toStream() => this.toStream(null);

        public KStream<K, V> toStream(string named)
        {
            string name = this.builder.newProcessorName(TOSTREAM_NAME);

            var p = new WrapperValueMapperWithKey<K, Change<V>, V>((k, v) => v.NewValue);
            IProcessorSupplier<K, Change<V>> processorMapValues = new KStreamMapValues<K, Change<V>, V>(p);
            ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<K, Change<V>>(processorMapValues, name);

            ProcessorGraphNode<K, Change<V>> toStreamNode = new ProcessorGraphNode<K, Change<V>>(name, processorParameters);

            builder.addGraphNode(this.node, toStreamNode);

            // we can inherit parent key and value serde
            return new KStreamImpl<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, toStreamNode, builder);
        }

        #endregion

        #region MapValues

        public KTable<K, VR> mapValues<VR>(Func<V, VR> mapper) 
            => this.mapValues(mapper, null);

        public KTable<K, VR> mapValues<VR>(Func<V, VR> mapper, string name)
            => this.mapValues(mapper, name, null);

        public KTable<K, VR> mapValues<VR>(Func<V, VR> mapper, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string name)
            => this.mapValues(mapper, name, materialized);

        public KTable<K, VR> mapValues<VR>(Func<V, VR> mapper, string name, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             => this.mapValues(new WrappedValueMapper<V, VR>(mapper), name, materialized);

        public KTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper)
            => this.mapValues(mapper, string.Empty);

        public KTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, string name)
            => this.mapValues(withKey(mapper), name);

        public KTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
            => this.mapValues(mapper, null, materialized);

        public KTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, string name, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
            => this.mapValues(withKey(mapper), name, materialized);

        public KTable<K, VR> mapValues<VR>(Func<K, V, VR> mapperWithKey)
            => this.mapValues(mapperWithKey, null);

        public KTable<K, VR> mapValues<VR>(Func<K, V, VR> mapperWithKey, string name)
            => this.mapValues(mapperWithKey, name, null);

        public KTable<K, VR> mapValues<VR>(Func<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string name)
            => this.mapValues(mapperWithKey, name, materialized);

        public KTable<K, VR> mapValues<VR>(Func<K, V, VR> mapperWithKey, string name, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
            => this.mapValues(new WrapperValueMapperWithKey<K, V, VR>(mapperWithKey), name, materialized);

        public KTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey)
            => this.mapValues(mapperWithKey, string.Empty);

        public KTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string name)
            => this.mapValues(mapperWithKey, name, null);

        public KTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
            => this.mapValues(mapperWithKey, null, materialized);

        public KTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string name, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
            => doMapValues(mapperWithKey, name, materialized);

        #endregion

        #region GroupBy

        public KGroupedTable<K1, V1> groupBy<K1, V1>(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> keySelector)
            => this.groupBy(keySelector, Grouped<K1, V1>.With(null, null));

        public KGroupedTable<K1, V1> groupBy<K1, V1>(Func<K, V, KeyValuePair<K1, V1>> keySelector)
            => this.groupBy(keySelector, Grouped<K1, V1>.With(null, null));

        public KGroupedTable<K1, V1> groupBy<K1, V1>(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> keySelector, Grouped<K1, V1> grouped)
            => doGroup(keySelector, grouped);

        public KGroupedTable<K1, V1> groupBy<K1, V1>(Func<K, V, KeyValuePair<K1, V1>> keySelector, Grouped<K1, V1> grouped)
            => this.groupBy(new WrappedKeyValueMapper<K, V, KeyValuePair<K1, V1>>(keySelector), grouped);

        #endregion

        #endregion

        #region Privates

        private KTable<K, V> doFilter(Func<K, V, bool> predicate, string named, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal, bool filterNot)
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
                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, V>(materializedInternal)).materialize() : null;
            }
            else
            {
                keySerde = this.keySerdes;
                valueSerde = this.valueSerdes;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var name = this.builder.newProcessorName(FILTER_NAME);

            IProcessorSupplier<K, Change<V>> processorSupplier = new KTableFilter<K, V>(this, predicate, filterNot, queryableStoreName);

            var processorParameters = new TableProcessorParameters<K, V>(processorSupplier, name);

            var tableNode = new TableProcessorNode<K, V, K, V>(
               name,
               processorParameters,
               storeBuilder
            );

            builder.addGraphNode(this.node, tableNode);

            return new KTableImpl<K, V, V>(name,
                                    keySerde,
                                    valueSerde,
                                    this.setSourceNodes,
                                    queryableStoreName,
                                    processorSupplier,
                                    tableNode,
                                    builder);
        }

        private KTable<K, VR> doMapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            ISerDes<K> keySerde;
            ISerDes<VR> valueSerde;
            String queryableStoreName;
            StoreBuilder<kafka_stream_core.State.TimestampedKeyValueStore<K, VR>> storeBuilder;

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at all since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    builder.newStoreName(MAPVALUES_NAME);
                }
                keySerde = materializedInternal.KeySerdes != null ? materializedInternal.KeySerdes : this.keySerdes;
                valueSerde = materializedInternal.ValueSerdes != null ? materializedInternal.ValueSerdes : null;
                queryableStoreName = materializedInternal.QueryableStoreName;
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, VR>(materializedInternal)).materialize() : null;
            }
            else
            {
                keySerde = this.keySerdes;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var name = this.builder.newProcessorName(MAPVALUES_NAME);

            var processorSupplier = new KTableMapValues<K, V, VR>(this, mapper, queryableStoreName);
            var processorParameters = new TableProcessorParameters<K, V>(processorSupplier, name);

            var tableNode = new TableProcessorNode<K, V, K, VR>(
               name,
               processorParameters,
               storeBuilder
            );

            builder.addGraphNode(this.node, tableNode);

            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            // we preserve the value following the order of 1) materialized, 2) null
            return new KTableImpl<K, V, VR>(
                name,
                keySerde,
                valueSerde,
                this.setSourceNodes,
                queryableStoreName,
                processorSupplier,
                tableNode,
                builder
            );
        }

        private KGroupedTable<K1, V1> doGroup<K1, V1>(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> keySelector, Grouped<K1, V1> grouped)
        {
            var selectName = this.builder.newProcessorName(SELECT_NAME);

            IKTableProcessorSupplier<K, V, KeyValuePair< K1, V1 >> selectSupplier = new KTableRepartitionMap<K, V, K1, V1>(this, keySelector);
            var processorParameters = new TableProcessorParameters<K, V>(selectSupplier, selectName);

            // select the aggregate key and values (old and new), it would require parent to send old values

            var groupByMapNode = new TableProcessorNode<K, V, K1, V1>(
               selectName,
               processorParameters,
               null
            );

            builder.addGraphNode(this.node, groupByMapNode);

            this.EnableSendingOldValues();

            return new KGroupedTableImpl<K1, V1>(
                selectName,
                grouped,
                this.setSourceNodes,
                groupByMapNode,
                builder);
        }

        #endregion

        #region Methods

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

        #endregion
    }
}