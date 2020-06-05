using System;
using System.Collections.Generic;
using System.Text;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
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
            this.windowOptions = windowOptions;
            this.aggBuilder = aggBuilder;
        }

        #region ITimeWindowedKStream Impl

        #region Count

        public IKTable<Windowed<K>, long> Count()
        {
            throw new NotImplementedException();
        }

        public IKTable<Windowed<K>, long> Count(string named)
        {
            throw new NotImplementedException();
        }

        public IKTable<Windowed<K>, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
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

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
        }

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
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

        public IKTable<K, V> Reduce(Reducer<V> reducer, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
        }

        public IKTable<K, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            throw new NotImplementedException();
        }


        #endregion

        #endregion
    }
}
