using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using System;

namespace kafka_stream_core.Stream
{
    public class Consumed<K, V>
    {
        internal ISerDes<K> KeySerdes { get; }
        internal ISerDes<V> ValueSerdes { get; }
        internal ITimestampExtractor TimestampExtractor { get; }
        internal Topology.AutoOffsetReset AutoOffsetReset { get; }


        internal Consumed(ISerDes<K> keySerdes, ISerDes<V> valueSerdes, ITimestampExtractor extractor, Topology.AutoOffsetReset autoOffsetReset)
        {
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            TimestampExtractor = extractor;
            AutoOffsetReset = autoOffsetReset;
        }

        #region Static

        public static Consumed<K, V> Create()
        {
            return new Consumed<K, V>(null, null, null, Topology.AutoOffsetReset.EARLIEST);
        }

        public static Consumed<K, V> Create<KS, VS>()
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return new Consumed<K, V>(new KS(), new VS(), null, Topology.AutoOffsetReset.EARLIEST);
        }

        public static Consumed<K, V> Create<KS, VS>(ITimestampExtractor extractor, Topology.AutoOffsetReset autoOffsetReset)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return new Consumed<K, V>(new KS(), new VS(), extractor, autoOffsetReset);
        }

        public static Consumed<K, V> Create<KS, VS>(ITimestampExtractor extractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return new Consumed<K, V>(new KS(), new VS(), extractor, Topology.AutoOffsetReset.EARLIEST);
        }

        public static Consumed<K, V> Create<KS, VS>(Topology.AutoOffsetReset autoOffsetReset)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return new Consumed<K, V>(new KS(), new VS(), null, autoOffsetReset);
        }

        #endregion
    }
}
