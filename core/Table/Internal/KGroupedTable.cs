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
using System.Text;

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

            if (materialized.StoreName == null)
            {
                builder.NewStoreName(KGroupedStream.AGGREGATE_NAME);
            }

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
            materialized = materialized ?? Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create();
            
            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(ValueSerdes);

             var aggregateSupplier = new KTableReduce<K, V>(
                materialized.StoreName,
                adder,
                substractor);

            return DoAggregate(aggregateSupplier, new Named(named), KGroupedTable.REDUCE_NAME, materialized);
        }

        public IKTable<K, V> Reduce(Func<V, V, V> adder, Func<V, V, V> substractor, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => Reduce(new WrappedReducer<V>(adder), new WrappedReducer<V>(substractor), Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create(), named);

        #endregion

        #region Aggregate

        public IKTable<K, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor)
        {
            throw new NotImplementedException();
        }

        public IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor)
        {
            throw new NotImplementedException();
        }

        public IKTable<K, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor) where VRS : ISerDes<VR>, new()
        {
            throw new NotImplementedException();
        }

        public IKTable<K, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor) where VRS : ISerDes<VR>, new()
        {
            throw new NotImplementedException();
        }

        public IKTable<K, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
        }

        public IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
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

            return DoAggregate(
                    new KTableAggregate<K, V, long>(
                        materialized.StoreName,
                        () => 0L,
                        (aggKey, value, aggregate) => aggregate + 1,
                        (aggKey, value, aggregate) => aggregate - 1),
                    new Named(named),
                    KGroupedTable.AGGREGATE_NAME,
                    materialized);
        }

        private IKTable<K, T> DoAggregate<T>(IProcessorSupplier<K, Change<V>> aggregateSupplier, Named named, string functionName, Materialized<K, T, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            materializedInternal.UseProvider(builder, KGroupedTable.AGGREGATE_NAME);
            var funcName = named.OrElseGenerateWithPrefix(builder, functionName);

            var processorParameters = new TableProcessorParameters<K, V>(aggregateSupplier, funcName);

            var statefulProcessorNode = new TableProcessorNode<K, V, K, T>(
                funcName,
                processorParameters,
                new TimestampedKeyValueStoreMaterializer<K, T>(materializedInternal).Materialize()
            );

            // now the repartition node must be the parent of the StateProcessorNode
            builder.AddGraphNode(Node, statefulProcessorNode);

            // return the KTable representation with the intermediate topic as the sources
            return new KTable<K, V, T>(funcName,
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
