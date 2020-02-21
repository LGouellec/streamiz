using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using kafka_stream_core.State;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal
{
    // TODO : 
    internal class KTableImpl<K, V> : AbstractStream<K, V>, KTable<K, V>
    {
        private readonly IProcessorSupplier<K, V> processorSupplier;

        private readonly string queryableStoreName;

        #region Constants

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

        internal KTableImpl(string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, String queryableStoreName, IProcessorSupplier<K, V> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            this.processorSupplier = processorSupplier;
            this.queryableStoreName = queryableStoreName;
        }

        #region Privates

        private KTable<K, V> doFilter(Func<K, V, bool> predicate, string named, Materialized<K, V, KeyValueStore<byte[], byte[]>> materializedInternal, bool filterNot)
        {
            //     Serde<K> keySerde;
            //     Serde<V> valueSerde;
            //     String queryableStoreName;
            //     StoreBuilder<TimestampedKeyValueStore< K, V >> storeBuilder;

            //    if (materializedInternal != null)
            //    {
            //        // we actually do not need to generate store names at all since if it is not specified, we will not
            //        // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
            //        if (materializedInternal.storeName() == null)
            //        {
            //            builder.newStoreName(FILTER_NAME);
            //        }
            //        // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
            //        // we preserve the key following the order of 1) materialized, 2) parent
            //        keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            //        // we preserve the value following the order of 1) materialized, 2) parent
            //        valueSerde = materializedInternal.valueSerde() != null ? materializedInternal.valueSerde() : this.valSerde;
            //        queryableStoreName = materializedInternal.queryableStoreName();
            //        // only materialize if materialized is specified and it has queryable name
            //        storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
            //    }
            //    else
            //    {
            //        keySerde = this.keySerde;
            //        valueSerde = this.valSerde;
            //        queryableStoreName = null;
            //        storeBuilder = null;
            //    }
            //     String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);

            //     KTableProcessorSupplier<K, V, V> processorSupplier =
            //        new KTableFilter<>(this, predicate, filterNot, queryableStoreName);

            //     ProcessorParameters<K, V> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            //        new ProcessorParameters<>(processorSupplier, name)
            //    );

            //     StreamsGraphNode tableNode = new TableProcessorNode<>(
            //        name,
            //        processorParameters,
            //        storeBuilder
            //    );

            //    builder.addGraphNode(this.streamsGraphNode, tableNode);

            //    return new KTableImpl<>(name,
            //                            keySerde,
            //                            valueSerde,
            //                            sourceNodes,
            //                            queryableStoreName,
            //                            processorSupplier,
            //                            tableNode,
            //                            builder);
            return null;
        }

        #endregion

        #region KTable Impl

        #region Filter

        public KTable<K, V> filter(Func<K, V, bool> predicate) => doFilter(predicate, null, null, false);

        public KTable<K, V> filter(Func<K, V, bool> predicate, string named) => doFilter(predicate, named, null, false);

        public KTable<K, V> filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized)
        {
            //MaterializedInternal<K, V, KeyValueStore< Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

            return doFilter(predicate, null, materialized, false);
        }

        public KTable<K, V> filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized, string named)
        {
            //MaterializedInternal<K, V, KeyValueStore< Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

            return doFilter(predicate, named, materialized, false);
        }

        public KTable<K, V> filterNot(Func<K, V, bool> predicate)
        {
            throw new NotImplementedException();
        }

        public KTable<K, V> filterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized)
        {
            throw new NotImplementedException();
        }

        public KTable<K, V> filterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized, string named)
        {
            throw new NotImplementedException();
        }

        #endregion

        public void @foreach(Action<K, V> action)
        {
            throw new NotImplementedException();
        }

        public void @foreach(Action<K, V> action, string named)
        {
            throw new NotImplementedException();
        }

        public KStream<K, V> toStream()
        {
            throw new NotImplementedException();
        }

        public KStream<K, V> toStream(string named)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
