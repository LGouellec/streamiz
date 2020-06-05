using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class KGroupedStream
    {
        //static string REDUCE_NAME = "KSTREAM-REDUCE-";
        internal static readonly string AGGREGATE_NAME = "KSTREAM-AGGREGATE-";
    }

    internal class KGroupedStream<K, V> : AbstractStream<K, V>, IKGroupedStream<K, V>
    {
        private readonly GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

        public KGroupedStream(string name, Grouped<K, V> grouped, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, grouped.Key, grouped.Value, sourceNodes, streamsGraphNode, builder)
        {
            aggregateBuilder = new GroupedStreamAggregateBuilder<K, V>(
                builder,
                grouped,
                sourceNodes,
                name,
                Node);
        }

        #region IKGroupedStream Impl

        #region Count

        public IKTable<K, long> Count()
            => Count(null);

        public IKTable<K, long> Count(string named)
            => Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>>.Create().With(KeySerdes, new Int64SerDes()));

        public IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            materialized = materialized ?? Materialized<K, long, IKeyValueStore<Bytes, byte[]>>.Create();

            return DoCount(materialized, named);
        }

        #endregion

        #region Aggregate

        public IKTable<K, VR> Aggregate<VR>(System.Func<VR> initializer, System.Func<K, V, VR, VR> aggregator)
            => Aggregate(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(aggregator));

        public IKTable<K, VR> Aggregate<VR, VRS>(System.Func<VR> initializer, System.Func<K, V, VR, VR> aggregator)
            where VRS : ISerDes<VR>, new()
            => Aggregate(
                new WrappedInitializer<VR>(initializer),
                new WrappedAggregator<K, V, VR>(aggregator),
                Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create().WithValueSerdes(new VRS()));

        public IKTable<K, VR> Aggregate<VR>(System.Func<VR> initializer, System.Func<K, V, VR, VR> aggregator, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => Aggregate(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(aggregator), materialized, named);

        public IKTable<K, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator)
            where VRS : ISerDes<VR>, new()
            => Aggregate(initializer, aggregator, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create().WithValueSerdes(new VRS()));

        public IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator)
            => Aggregate(initializer, aggregator, null);

        public IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            CheckIfParamNull(initializer, "initializer");
            CheckIfParamNull(aggregator, "aggregator");

            materialized = materialized ?? Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create();

            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedStream.AGGREGATE_NAME);
            materialized.UseProvider(builder, KGroupedStream.AGGREGATE_NAME);
            return DoAggregate(
                    new KStreamAggregate<K, V, VR>(materialized.StoreName, initializer, aggregator),
                    name,
                    materialized);
        }

        #endregion

        #region Reduce

        public IKTable<K, V> Reduce(Reducer<V> reducer)
            => Reduce(reducer, null);

        public IKTable<K, V> Reduce(Func<V, V, V> reducer)
            => Reduce(reducer, null);


        public IKTable<K, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
            => Reduce(new WrappedReducer<V>(reducer), materialized, named);

        public IKTable<K, V> Reduce(Reducer<V> reducer, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            CheckIfParamNull(reducer, "reducer");
            materialized = materialized ?? Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create();

            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);
            
            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(ValueSerdes);

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedStream.AGGREGATE_NAME);
            materialized.UseProvider(builder, KGroupedStream.AGGREGATE_NAME);
            
            return DoAggregate(
                    new KStreamReduce<K, V>(materialized.StoreName, reducer),
                    name,
                    materialized
            );
        }

        #endregion

        #region Windows
        public ITimeWindowedKStream<K, V> WindowedBy<W>(WindowOptions<W> options) where W : Window
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

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedStream.AGGREGATE_NAME);
            materialized.UseProvider(builder, KGroupedStream.AGGREGATE_NAME);

            return DoAggregate(
                    new KStreamAggregate<K, V, long>(
                        materialized.StoreName,
                        () => 0L,
                        (aggKey, value, aggregate) => aggregate + 1),
                    name,
                    materialized);
        }

        private IKTable<K, T> DoAggregate<T>(IKStreamAggProcessorSupplier<K, K, V, T> aggregateSupplier, string functionName, Materialized<K, T, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            return aggregateBuilder.Build(
                functionName,
                new TimestampedKeyValueStoreMaterializer<K, T>(materializedInternal).Materialize(),
                aggregateSupplier,
                materializedInternal.QueryableStoreName,
                materializedInternal.KeySerdes,
                materializedInternal.ValueSerdes);
        }

        #endregion
    }
}
