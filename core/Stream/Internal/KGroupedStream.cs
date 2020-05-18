using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;
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
                node);
        }

        #region IKGroupedStream Impl

        #region Count

        public IKTable<K, long> Count()
            => Count(null);

        public IKTable<K, long> Count(string named)
            => Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>>.Create().With(keySerdes, new Int64SerDes()));

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

        #endregion

        #region Private

        private IKTable<K, long> DoCount(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(keySerdes);

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
