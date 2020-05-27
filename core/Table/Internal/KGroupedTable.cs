using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table.Internal.Graph;
using Streamiz.Kafka.Net.Table.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KGroupedTable
    {
        internal static string AGGREGATE_NAME = "KTABLE-AGGREGATE-";

        internal static string REDUCE_NAME = "KTABLE-REDUCE-";
    }

    internal class KGroupedTable<K, V> : AbstractStream<K, V>, IKGroupedTable<K, V>
    {
        public KGroupedTable(string name, Grouped<K, V> grouped, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, grouped.Key, grouped.Value, sourceNodes, streamsGraphNode, builder)
        {
        }

        #region  IKGroupedTable Impl

        #region Count

        public IKTable<K, long> Count() => Count(null);

        public IKTable<K, long> Count(string named)
            => Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>>.Create().With(KeySerdes, new Int64SerDes()), named);

        public IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            materialized = materialized ?? Materialized<K, long, IKeyValueStore<Bytes, byte[]>>.Create();

            return DoCount(materialized, named);
        }

        #endregion

        #region Reducer

        public IKTable<K, V> Reduce(Reducer<V> adder, Reducer<V> substractor)
            => Reduce(adder, substractor, Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create(), null);

        public IKTable<K, V> Reduce(Func<V, V, V> adder, Func<V, V, V> substractor)
            => Reduce(new WrappedReducer<V>(adder), new WrappedReducer<V>(substractor), Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create(), null);

        public IKTable<K, V> Reduce(Reducer<V> adder, Reducer<V> substractor, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            CheckIfParamNull(adder, "adder");
            CheckIfParamNull(substractor, "substractor");

            materialized = materialized ?? Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create();

            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(ValueSerdes);

            var funcName = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedTable.REDUCE_NAME);
            materialized.UseProvider(builder, KGroupedTable.REDUCE_NAME);

            var aggregateSupplier = new KTableReduce<K, V>(
                materialized.StoreName,
                adder,
                substractor);

            return DoAggregate(aggregateSupplier, funcName, materialized);
        }

        public IKTable<K, V> Reduce(Func<V, V, V> adder, Func<V, V, V> substractor, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => Reduce(new WrappedReducer<V>(adder), new WrappedReducer<V>(substractor), materialized, named);

        #endregion

        #region Aggregate

        public IKTable<K, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor)
            => Aggregate(initializer, adder, subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create());

        public IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor)
            => Aggregate(initializer, adder, subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create());

        public IKTable<K, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor) where VRS : ISerDes<VR>, new()
            => Aggregate(initializer, adder, subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create().WithValueSerdes(new VRS()));

        public IKTable<K, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor) where VRS : ISerDes<VR>, new()
            => Aggregate(initializer, adder, subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create().WithValueSerdes(new VRS()));

        public IKTable<K, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => Aggregate(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(adder), new WrappedAggregator<K, V, VR>(subtractor), materialized, named);

        public IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            CheckIfParamNull(initializer, "initializer");
            CheckIfParamNull(adder, "adder");
            CheckIfParamNull(subtractor, "subtractor");

            materialized = materialized ?? Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create();

            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            var funcName = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedTable.AGGREGATE_NAME);
            materialized.UseProvider(builder, KGroupedTable.AGGREGATE_NAME);

            var aggregateSupplier = new KTableAggregate<K, V, VR>(
                                        materialized.StoreName,
                                        initializer,
                                        adder,
                                        subtractor);
            return DoAggregate(aggregateSupplier, funcName, materialized);
        }


        #endregion

        #endregion

        #region Private

        private IKTable<K, long> DoCount(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(new Int64SerDes());

            var funcName = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedTable.AGGREGATE_NAME);
            materialized.UseProvider(builder, KGroupedTable.AGGREGATE_NAME);

            return DoAggregate(
                    new KTableAggregate<K, V, long>(
                        materialized.StoreName,
                        () => 0L,
                        (aggKey, value, aggregate) => aggregate + 1,
                        (aggKey, value, aggregate) => aggregate - 1),
                    funcName,
                    materialized);
        }

        private IKTable<K, T> DoAggregate<T>(IProcessorSupplier<K, Change<V>> aggregateSupplier, string functionName, Materialized<K, T, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            var processorParameters = new TableProcessorParameters<K, V>(aggregateSupplier, functionName);

            var statefulProcessorNode = new TableProcessorNode<K, V, K, T>(
                functionName,
                processorParameters,
                new TimestampedKeyValueStoreMaterializer<K, T>(materializedInternal).Materialize()
            );

            // now the repartition node must be the parent of the StateProcessorNode
            builder.AddGraphNode(Node, statefulProcessorNode);

            // return the KTable representation with the intermediate topic as the sources
            return new KTable<K, V, T>(functionName,
                                    materializedInternal.KeySerdes,
                                    materializedInternal.ValueSerdes,
                                    SetSourceNodes,
                                    materializedInternal.QueryableStoreName,
                                    aggregateSupplier,
                                    statefulProcessorNode,
                                    builder);
        }

        #endregion
    }
}
