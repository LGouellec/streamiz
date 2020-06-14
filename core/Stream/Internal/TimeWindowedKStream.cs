using System;
using System.Collections.Generic;
using System.Text;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class TimeWindowedKStream<K, V, W> : AbstractStream<K, V>, ITimeWindowedKStream<K, V>
        where W : Window
    {
        private readonly WindowOptions<W> windowOptions;
        private readonly GroupedStreamAggregateBuilder<K, V> aggBuilder;

        public TimeWindowedKStream(WindowOptions<W> windowOptions, GroupedStreamAggregateBuilder<K, V> aggBuilder, string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder) 
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            CheckIfParamNull(windowOptions, "windowOptions");

            this.windowOptions = windowOptions;
            this.aggBuilder = aggBuilder;
        }

        #region ITimeWindowedKStream Impl

        #region Count

        public IKTable<Windowed<K>, long> Count()
             => Count((string)null);

        public IKTable<Windowed<K>, long> Count(string named)
            => Count(Materialized<K, long, WindowStore<Bytes, byte[]>>.Create(), named);

        public IKTable<Windowed<K>, long> Count(Materialized<K, long, WindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            materialized = materialized ?? Materialized<K, long, WindowStore<Bytes, byte[]>>.Create();

            return DoCount(materialized, named);
        }

        #endregion

        #region Aggregate

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator)
        {
            throw new NotImplementedException();
        }

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator)
        {
            throw new NotImplementedException();
        }

        public IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> aggregator) where VRS : ISerDes<VR>, new()
        {
            throw new NotImplementedException();
        }

        public IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator) where VRS : ISerDes<VR>, new()
        {
            throw new NotImplementedException();
        }

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
        }

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Reduce

        public IKTable<K, V> Reduce(Reducer<V> reducer)
        {
            throw new NotImplementedException();
        }

        public IKTable<K, V> Reduce(Func<V, V, V> reducer)
        {
            throw new NotImplementedException();
        }

        public IKTable<K, V> Reduce(Reducer<V> reducer, Materialized<K, V, WindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
        }

        public IKTable<K, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, WindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
        }


        #endregion

        #endregion

        #region Privates

        private IKTable<Windowed<K>, long> DoCount(Materialized<K, long, WindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(new Int64SerDes());

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedStream.AGGREGATE_NAME);
            materialized.UseProvider(builder, KGroupedStream.AGGREGATE_NAME);

            var aggSupplier = new KStreamWindowAggregate<K, V, long, W>(
                windowOptions,
                materialized.StoreName,
                () => 0L,
                (aggKey, value, aggregate) => aggregate + 1);

            ISerDes<Windowed<K>> windowSerdes = materialized.KeySerdes != null ? new TimeWindowedSerDes<K>(materialized.KeySerdes, windowOptions.Size) : null;

            return aggBuilder.BuildWindow(name,
                                    new TimestampedWindowStoreMaterializer<K, long, W>(windowOptions, materialized).Materialize(),
                                    aggSupplier,
                                    materialized.QueryableStoreName,
                                    windowSerdes,
                                    materialized.ValueSerdes);
        }

        #endregion
    }
}
