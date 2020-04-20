using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table.Internal.Graph;
using Streamiz.Kafka.Net.Table.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KTable<K, S, V> : AbstractStream<K, V>, IKTable<K, V>, IKTableGetter<K, V>
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

        internal KTable(string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, String queryableStoreName, IProcessorSupplier<K, Change<S>> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.tableProcessorSupplier = processorSupplier;
            this.queryableStoreName = queryableStoreName;
        }

        internal KTable(string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, String queryableStoreName, IProcessorSupplier<K, S> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.processorSupplier = processorSupplier;
            this.queryableStoreName = queryableStoreName;
        }

        #region KTable Impl

        #region Filter

        public IKTable<K, V> Filter(Func<K, V, bool> predicate, string named = null) => DoFilter(predicate, named, null, false);

        public IKTable<K, V> Filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => DoFilter(predicate, named, materialized, false);

        public IKTable<K, V> FilterNot(Func<K, V, bool> predicate, string named = null) => DoFilter(predicate, named, null, true);

        public IKTable<K, V> FilterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => DoFilter(predicate, named, materialized, true);

        #endregion

        #region ToStream

        public IKStream<KR, V> ToStream<KR>(Func<K, V, KR> mapper, string named = null)
            => this.ToStream(new WrappedKeyValueMapper<K, V, KR>(mapper), named);

        public IKStream<KR, V> ToStream<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null)
        {
            return ToStream().SelectKey(mapper, named);
        }

        public IKStream<K, V> ToStream(string named = null)
        {
            string name = new Named(named).OrElseGenerateWithPrefix(this.builder, TOSTREAM_NAME);

            var p = new WrapperValueMapperWithKey<K, Change<V>, V>((k, v) => v.NewValue);
            IProcessorSupplier<K, Change<V>> processorMapValues = new KStreamMapValues<K, Change<V>, V>(p);
            ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<K, Change<V>>(processorMapValues, name);

            ProcessorGraphNode<K, Change<V>> toStreamNode = new ProcessorGraphNode<K, Change<V>>(name, processorParameters);

            builder.AddGraphNode(this.node, toStreamNode);

            // we can inherit parent key and value serde
            return new KStream<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, toStreamNode, builder);
        }

        #endregion

        #region MapValues

        public IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper) 
            => this.MapValues(mapper, null);

        public IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, string named = null)
            => this.MapValues(mapper, null, named);


        public IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string named = null)
             => this.MapValues(new WrappedValueMapper<V, VR>(mapper), materialized, named);

        public IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named = null)
            => this.MapValues(WithKey(mapper), named);

        public IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => this.MapValues(WithKey(mapper), materialized, named);

        public IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, string named = null)
            => this.MapValues(mapperWithKey, null, named);

        public IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => this.MapValues(new WrapperValueMapperWithKey<K, V, VR>(mapperWithKey), materialized, named);

        public IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string named = null)
            => this.MapValues(mapperWithKey, null, named);

        public IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string named)
            => DoMapValues(mapperWithKey, named, materialized);

        #endregion

        #region GroupBy

        public IKGroupedTable<KR, VR> GroupBy<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> keySelector, string named = null)
            => DoGroup(keySelector, Grouped<KR, VR>.Create(named, null, null));

        public IKGroupedTable<KR, VR> GroupBy<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> keySelector, string named = null)
            => this.GroupBy(new WrappedKeyValueMapper<K, V, KeyValuePair<KR, VR>>(keySelector), named);

        public IKGroupedTable<KR, VR> GroupBy<KR, VR, KRS, VRS>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> keySelector, string named = null)
            where KRS : ISerDes<KR>, new()
            where VRS : ISerDes<VR>, new()
            => DoGroup(keySelector, Grouped<KR, VR>.Create<KRS, VRS>(named));

        public IKGroupedTable<KR, VR> GroupBy<KR, VR, KRS, VRS>(Func<K, V, KeyValuePair<KR, VR>> keySelector, string named = null)
            where KRS : ISerDes<KR>, new()
            where VRS : ISerDes<VR>, new()
            => this.GroupBy(new WrappedKeyValueMapper<K, V, KeyValuePair<KR, VR>>(keySelector), named);

        #endregion

        #endregion

        #region Privates

        private IKTable<K, V> DoFilter(Func<K, V, bool> predicate, string named, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal, bool filterNot)
        {
            ISerDes<K> keySerde;
            ISerDes<V> valueSerde;
            String queryableStoreName;
            StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder;

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at all since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    builder.NewStoreName(FILTER_NAME);
                }
                // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
                // we preserve the key following the order of 1) materialized, 2) parent
                keySerde = materializedInternal.KeySerdes != null ? materializedInternal.KeySerdes : this.keySerdes;
                // we preserve the value following the order of 1) materialized, 2) parent
                valueSerde = materializedInternal.ValueSerdes != null ? materializedInternal.ValueSerdes : this.valueSerdes;

                queryableStoreName = materializedInternal.QueryableStoreName;
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, V>(materializedInternal)).Materialize() : null;
            }
            else
            {
                keySerde = this.keySerdes;
                valueSerde = this.valueSerdes;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var name = new Named(named).OrElseGenerateWithPrefix(this.builder, FILTER_NAME);

            IProcessorSupplier<K, Change<V>> processorSupplier = new KTableFilter<K, V>(this, predicate, filterNot, queryableStoreName);

            var processorParameters = new TableProcessorParameters<K, V>(processorSupplier, name);

            var tableNode = new TableProcessorNode<K, V, K, V>(
               name,
               processorParameters,
               storeBuilder
            );

            builder.AddGraphNode(this.node, tableNode);

            return new KTable<K, V, V>(name,
                                    keySerde,
                                    valueSerde,
                                    this.setSourceNodes,
                                    queryableStoreName,
                                    processorSupplier,
                                    tableNode,
                                    builder);
        }

        private IKTable<K, VR> DoMapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            ISerDes<K> keySerde;
            ISerDes<VR> valueSerde;
            String queryableStoreName;
            StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at all since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    builder.NewStoreName(MAPVALUES_NAME);
                }
                keySerde = materializedInternal.KeySerdes != null ? materializedInternal.KeySerdes : this.keySerdes;
                valueSerde = materializedInternal.ValueSerdes != null ? materializedInternal.ValueSerdes : null;
                queryableStoreName = materializedInternal.QueryableStoreName;
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? new TimestampedKeyValueStoreMaterializer<K, VR>(materializedInternal).Materialize() : null;
            }
            else
            {
                keySerde = this.keySerdes;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var name = new Named(named).OrElseGenerateWithPrefix(this.builder, MAPVALUES_NAME);

            var processorSupplier = new KTableMapValues<K, V, VR>(this, mapper, queryableStoreName);
            var processorParameters = new TableProcessorParameters<K, V>(processorSupplier, name);

            var tableNode = new TableProcessorNode<K, V, K, VR>(
               name,
               processorParameters,
               storeBuilder
            );

            builder.AddGraphNode(this.node, tableNode);

            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            // we preserve the value following the order of 1) materialized, 2) null
            return new KTable<K, V, VR>(
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

        private IKGroupedTable<K1, V1> DoGroup<K1, V1>(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> keySelector, Grouped<K1, V1> grouped)
        {
            var selectName = new Named(grouped.Named).OrElseGenerateWithPrefix(this.builder, SELECT_NAME);

            IKTableProcessorSupplier<K, V, KeyValuePair< K1, V1 >> selectSupplier = new KTableRepartitionMap<K, V, K1, V1>(this, keySelector);
            var processorParameters = new TableProcessorParameters<K, V>(selectSupplier, selectName);

            // select the aggregate key and values (old and new), it would require parent to send old values

            var groupByMapNode = new TableProcessorNode<K, V, K1, V1>(
               selectName,
               processorParameters,
               null
            );

            builder.AddGraphNode(this.node, groupByMapNode);

            this.EnableSendingOldValues();

            return new KGroupedTable<K1, V1>(
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
                    source.EnableSendingOldValues();
                }
                // TODO : 
                //} else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                //    ((KStreamAggProcessorSupplier <?, K, S, V >) processorSupplier).enableSendingOldValues();
                //} else
                else if(tableProcessorSupplier != null && tableProcessorSupplier is IKTableProcessorSupplier)
                {
                    (tableProcessorSupplier as IKTableProcessorSupplier).EnableSendingOldValues();
                }
                SendOldValues = true;
            }
        }

        #endregion
    }
}