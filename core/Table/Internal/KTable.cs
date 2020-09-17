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
    internal static class KTable
    {
        #region Constants

        internal static readonly string FOREACH_NAME = "KTABLE-FOREACH-";
        internal static readonly string SOURCE_NAME = "KTABLE-SOURCE-";
        internal static readonly string STATE_STORE_NAME = "STATE-STORE-";
        internal static readonly string FILTER_NAME = "KTABLE-FILTER-";
        internal static readonly string JOINTHIS_NAME = "KTABLE-JOINTHIS-";
        internal static readonly string JOINOTHER_NAME = "KTABLE-JOINOTHER-";
        internal static readonly string MAPVALUES_NAME = "KTABLE-MAPVALUES-";
        internal static readonly string MERGE_NAME = "KTABLE-MERGE-";
        internal static readonly string SELECT_NAME = "KTABLE-SELECT-";
        internal static readonly string SUPPRESS_NAME = "KTABLE-SUPPRESS-";
        internal static readonly string TOSTREAM_NAME = "KTABLE-TOSTREAM-";
        internal static readonly string TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";
        internal static readonly string SINK_NAME = "KTABLE-SINK-";
        internal static readonly string FK_JOIN = "KTABLE-FK-JOIN-";
        internal static readonly string FK_JOIN_STATE_STORE_NAME = FK_JOIN + "SUBSCRIPTION-STATE-STORE-";
        internal static readonly string SUBSCRIPTION_REGISTRATION = FK_JOIN + "SUBSCRIPTION-REGISTRATION-";
        internal static readonly string SUBSCRIPTION_RESPONSE = FK_JOIN + "SUBSCRIPTION-RESPONSE-";
        internal static readonly string SUBSCRIPTION_PROCESSOR = FK_JOIN + "SUBSCRIPTION-PROCESSOR-";
        internal static readonly string SUBSCRIPTION_RESPONSE_RESOLVER_PROCESSOR = FK_JOIN + "SUBSCRIPTION-RESPONSE-RESOLVER-PROCESSOR-";
        internal static readonly string FK_JOIN_OUTPUT_NAME = FK_JOIN + "OUTPUT-";
        internal static readonly string TOPIC_SUFFIX = "-topic";

        #endregion
    }

    internal class KTable<K, S, V> : AbstractStream<K, V>, IKTable<K, V>, IKTableGetter<K, V>
    {
        private readonly IProcessorSupplier<K, S> processorSupplier = null;
        private readonly IProcessorSupplier<K, Change<S>> tableProcessorSupplier = null;
        private readonly string queryableStoreName;

        public bool SendOldValues { get; protected set; }

        public virtual IKTableValueGetterSupplier<K, V> ValueGetterSupplier
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
                else if (processorSupplier is IKStreamAggProcessorSupplier<K, V>)
                {
                    return ((IKStreamAggProcessorSupplier<K, V>)processorSupplier).View();
                }
                else if (tableProcessorSupplier != null && tableProcessorSupplier is IKTableProcessorSupplier<K, S, V>)
                {
                    return ((IKTableProcessorSupplier<K, S, V>)tableProcessorSupplier).View;
                }
                else
                    return null;
            }
        }

        internal KTable(string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, String queryableStoreName, IProcessorSupplier<K, Change<S>> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            tableProcessorSupplier = processorSupplier;
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

        public IKTable<K, V> Filter(Func<K, V, bool> predicate, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => DoFilter(predicate, named, materialized, false);

        public IKTable<K, V> FilterNot(Func<K, V, bool> predicate, string named = null) => DoFilter(predicate, named, null, true);

        public IKTable<K, V> FilterNot(Func<K, V, bool> predicate, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => DoFilter(predicate, named, materialized, true);

        #endregion

        #region ToStream

        public IKStream<KR, V> ToStream<KR>(Func<K, V, KR> mapper, string named = null)
            => ToStream(new WrappedKeyValueMapper<K, V, KR>(mapper), named);

        public IKStream<KR, V> ToStream<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null)
        {
            return ToStream().SelectKey(mapper, named);
        }

        public IKStream<K, V> ToStream(string named = null)
        {
            string name = new Named(named).OrElseGenerateWithPrefix(builder, KTable.TOSTREAM_NAME);

            var p = new WrappedValueMapperWithKey<K, Change<V>, V>((k, v) => v.NewValue);
            IProcessorSupplier<K, Change<V>> processorMapValues = new KStreamMapValues<K, Change<V>, V>(p);
            ProcessorParameters<K, Change<V>> processorParameters = new ProcessorParameters<K, Change<V>>(processorMapValues, name);

            ProcessorGraphNode<K, Change<V>> toStreamNode = new ProcessorGraphNode<K, Change<V>>(name, processorParameters);

            builder.AddGraphNode(Node, toStreamNode);

            // we can inherit parent key and value serde
            return new KStream<K, V>(name, KeySerdes, ValueSerdes, SetSourceNodes, toStreamNode, builder);
        }

        #endregion

        #region MapValues

        public IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper)
            => MapValues(mapper, null);

        public IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, string named = null)
            => MapValues(mapper, null, named);


        public IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
             => MapValues(new WrappedValueMapper<V, VR>(mapper), materialized, named);

        public IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named = null)
            => MapValues(WithKey(mapper), named);

        public IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => MapValues(WithKey(mapper), materialized, named);

        public IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, string named = null)
            => MapValues(mapperWithKey, null, named);

        public IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => MapValues(new WrappedValueMapperWithKey<K, V, VR>(mapperWithKey), materialized, named);

        public IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string named = null)
            => MapValues(mapperWithKey, null, named);

        public IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named)
            => DoMapValues(mapperWithKey, named, materialized);

        #endregion

        #region GroupBy

        public IKGroupedTable<KR, VR> GroupBy<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> keySelector, string named = null)
            => DoGroup(keySelector, Grouped<KR, VR>.Create(named, null, null));

        public IKGroupedTable<KR, VR> GroupBy<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> keySelector, string named = null)
            => GroupBy(new WrappedKeyValueMapper<K, V, KeyValuePair<KR, VR>>(keySelector), named);

        public IKGroupedTable<KR, VR> GroupBy<KR, VR, KRS, VRS>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> keySelector, string named = null)
            where KRS : ISerDes<KR>, new()
            where VRS : ISerDes<VR>, new()
            => DoGroup(keySelector, Grouped<KR, VR>.Create<KRS, VRS>(named));

        public IKGroupedTable<KR, VR> GroupBy<KR, VR, KRS, VRS>(Func<K, V, KeyValuePair<KR, VR>> keySelector, string named = null)
            where KRS : ISerDes<KR>, new()
            where VRS : ISerDes<VR>, new()
            => GroupBy(new WrappedKeyValueMapper<K, V, KeyValuePair<KR, VR>>(keySelector), named);

        #endregion

        //#region Join

        //public IKTable<K, VR> Join<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner)
        //    => Join<VT, VR, VTS, VRS>(table, valueJoiner, null);

        //public IKTable<K, VR> Join<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        //    => Join<VT, VR, VTS, VRS>(table, new WrappedValueJoiner<V, VT, VR>(valueJoiner), materialized, named);

        //public IKTable<K, VR> Join<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner)
        //    => Join<VT, VR>(table, valueJoiner, null);

        //public IKTable<K, VR> Join<VT, VR>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner)
        //    => Join<VT, VR>(table, valueJoiner, null);

        //public IKTable<K, VR> Join<VT, VR>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        //    => Join<VT, VR>(table, new WrappedValueJoiner<V, VT, VR>(valueJoiner), materialized, named);

        //public IKTable<K, VR> Join<VT, VR, VTS, VRS>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner)
        //    => Join<VT, VR>(table, valueJoiner, null);

        //public IKTable<K, VR> Join<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        //{
        //    throw new NotImplementedException();
        //    // => DoJoin<VT, VR>(table, valueJoiner, named, materialized, false, false);
        //}

        //public IKTable<K, VR> Join<VT, VR, VTS, VRS>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        //{
        //    throw new NotImplementedException();
        //    // => DoJoin<VT, VR>(table, valueJoiner, named, materialized, false, false);
        //}

        //#endregion

        //#region LeftJoin

        //public IKTable<K, VR> LeftJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner)
        //{
        //    throw new NotImplementedException();
        //}

        //public IKTable<K, VR> LeftJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        //{
        //    throw new NotImplementedException();
        //}

        //#endregion

        //#region Outer Join

        //public IKTable<K, VR> OuterJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner)
        //{
        //    throw new NotImplementedException();
        //}

        //public IKTable<K, VR> OuterJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        //{
        //    throw new NotImplementedException();
        //}

        //#endregion

        #endregion

        #region Privates

        private IKTable<K, V> DoFilter(Func<K, V, bool> predicate, string named, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal, bool filterNot)
        {
            ISerDes<K> keySerde;
            ISerDes<V> valueSerde;
            String queryableStoreName;
            StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder;

            if (predicate == null)
                throw new ArgumentNullException($"Filter() doesn't allow null predicate function");

            if (materializedInternal != null)
            {
                // we actually do not need to generate store names at all since if it is not specified, we will not
                // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
                if (materializedInternal.StoreName == null)
                {
                    builder.NewStoreName(KTable.FILTER_NAME);
                }
                // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
                // we preserve the key following the order of 1) materialized, 2) parent
                keySerde = materializedInternal.KeySerdes != null ? materializedInternal.KeySerdes : KeySerdes;
                // we preserve the value following the order of 1) materialized, 2) parent
                valueSerde = materializedInternal.ValueSerdes != null ? materializedInternal.ValueSerdes : ValueSerdes;
                // ONLY FOR CALCULATE PROPERTY queriable
                materializedInternal.UseProvider(null, null);
                queryableStoreName = materializedInternal.QueryableStoreName;
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<K, V>(materializedInternal)).Materialize() : null;
            }
            else
            {
                keySerde = KeySerdes;
                valueSerde = ValueSerdes;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var name = new Named(named).OrElseGenerateWithPrefix(builder, KTable.FILTER_NAME);

            IProcessorSupplier<K, Change<V>> processorSupplier = new KTableFilter<K, V>(this, predicate, filterNot, queryableStoreName);

            var processorParameters = new TableProcessorParameters<K, V>(processorSupplier, name);

            var tableNode = new TableProcessorNode<K, V, K, V>(
               name,
               processorParameters,
               storeBuilder
            );

            builder.AddGraphNode(Node, tableNode);

            return new KTable<K, V, V>(name,
                                    keySerde,
                                    valueSerde,
                                    SetSourceNodes,
                                    queryableStoreName,
                                    processorSupplier,
                                    tableNode,
                                    builder);
        }

        private IKTable<K, VR> DoMapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            if (mapper == null)
                throw new ArgumentNullException($"MapValues() doesn't allow null mapper function");

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
                    builder.NewStoreName(KTable.MAPVALUES_NAME);
                }
                keySerde = materializedInternal.KeySerdes != null ? materializedInternal.KeySerdes : KeySerdes;
                valueSerde = materializedInternal.ValueSerdes != null ? materializedInternal.ValueSerdes : null;
                // ONLY FOR CALCULATE PROPERTY queriable 
                materializedInternal.UseProvider(null, null);
                queryableStoreName = materializedInternal.QueryableStoreName;
                // only materialize if materialized is specified and it has queryable name
                storeBuilder = queryableStoreName != null ? new TimestampedKeyValueStoreMaterializer<K, VR>(materializedInternal).Materialize() : null;
            }
            else
            {
                keySerde = KeySerdes;
                valueSerde = null;
                queryableStoreName = null;
                storeBuilder = null;
            }

            var name = new Named(named).OrElseGenerateWithPrefix(builder, KTable.MAPVALUES_NAME);

            var processorSupplier = new KTableMapValues<K, V, VR>(this, mapper, queryableStoreName);
            var processorParameters = new TableProcessorParameters<K, V>(processorSupplier, name);

            var tableNode = new TableProcessorNode<K, V, K, VR>(
               name,
               processorParameters,
               storeBuilder
            );

            builder.AddGraphNode(Node, tableNode);

            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            // we preserve the value following the order of 1) materialized, 2) null
            return new KTable<K, V, VR>(
                name,
                keySerde,
                valueSerde,
                SetSourceNodes,
                queryableStoreName,
                processorSupplier,
                tableNode,
                builder
            );
        }

        private IKGroupedTable<K1, V1> DoGroup<K1, V1>(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> keySelector, Grouped<K1, V1> grouped)
        {
            if (keySelector == null)
                throw new ArgumentNullException("GroupBy() doesn't allow null selector function");

            var selectName = new Named(grouped.Named).OrElseGenerateWithPrefix(builder, KTable.SELECT_NAME);

            IKTableProcessorSupplier<K, V, KeyValuePair<K1, V1>> selectSupplier = new KTableRepartitionMap<K, V, K1, V1>(this, keySelector);
            var processorParameters = new TableProcessorParameters<K, V>(selectSupplier, selectName);

            // select the aggregate key and values (old and new), it would require parent to send old values

            var groupByMapNode = new TableProcessorNode<K, V, K1, V1>(
               selectName,
               processorParameters,
               null
            );

            builder.AddGraphNode(Node, groupByMapNode);

            EnableSendingOldValues();

            return new KGroupedTable<K1, V1>(
                selectName,
                grouped,
                SetSourceNodes,
                groupByMapNode,
                builder);
        }

        //private IKTable<K, VR> DoJoin<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> joiner, string named, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal, bool leftOuter, bool rightOuter)
        //{
        //    CheckIfParamNull(table, "table");
        //    CheckIfParamNull(joiner, "joiner");

        //    materializedInternal = materializedInternal ?? Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create();

        //    var joinMergeName = new Named(named).OrElseGenerateWithPrefix(builder, KTable.MERGE_NAME);

        //    if (leftOuter)
        //        EnableSendingOldValues();
        //    if (rightOuter)
        //        (table as IKTableGetter<K, V0>)?.EnableSendingOldValues();

        //    //AbstractKTableKTableJoin<K, VR, V, V0> joinLeft = null;
        //    //AbstractKTableKTableJoin<K, VR, V0, V> joinRigth = null;

        //    if (!leftOuter) // INNER JOIN
        //    {
                
        //    }
        //    else if(!rightOuter) // LEFT JOIN
        //    {

        //    }
        //    else // OUTER JOIN
        //    {

        //    }

        //    return null;
        //}

        #endregion

        #region Methods

        public virtual void EnableSendingOldValues()
        {
            if (!SendOldValues)
            {
                if (processorSupplier != null && processorSupplier is KTableSource<K, V>)
                {
                    KTableSource<K, V> source = (KTableSource<K, V>)processorSupplier;
                    source.EnableSendingOldValues();
                }
                else if (processorSupplier is IKStreamAggProcessorSupplier<K, V>)
                {
                    ((IKStreamAggProcessorSupplier<K, V>)processorSupplier).EnableSendingOldValues();
                }
                else if (tableProcessorSupplier != null && tableProcessorSupplier is IKTableProcessorSupplier)
                {
                    (tableProcessorSupplier as IKTableProcessorSupplier).EnableSendingOldValues();
                }
                SendOldValues = true;
            }
        }

        #endregion
    }
}