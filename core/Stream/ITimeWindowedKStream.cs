using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream
{
    public interface ITimeWindowedKStream<K, V>
    {
        IKTable<Windowed<K>, long> Count();
        IKTable<Windowed<K>, long> Count(string named);
        IKTable<Windowed<K>, long> Count(Materialized<K, long, WindowStore<Bytes, byte[]>> materialized, string named = null);

        IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator);
        IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator);
        IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> aggregator) where VRS : ISerDes<VR>, new();
        IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator) where VRS : ISerDes<VR>, new();
        IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized, string named = null);
        IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized, string named = null);

        IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer);
        IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer);
        IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer, Materialized<K, V, WindowStore<Bytes, byte[]>> materialized, string named = null);
        IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, WindowStore<Bytes, byte[]>> materialized, string named = null);
    }
}
