using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Stream
{
    public class Consumed<K,V>
    {
        internal ISerDes<K> KeySerdes { get; }
        internal ISerDes<V> ValueSerdes { get; }
        internal TimestampExtractor TimestampExtractor { get; }
        internal Topology.AutoOffsetReset AutoOffsetReset { get; }


        internal Consumed(ISerDes<K> keySerdes, ISerDes<V> valueSerdes, TimestampExtractor extractor, Topology.AutoOffsetReset autoOffsetReset)
        {
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            TimestampExtractor = extractor;
            AutoOffsetReset = autoOffsetReset;
        }

        #region Static

        public static Consumed<K,V> with(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            return new Consumed<K, V>(keySerdes, valueSerdes, null, Topology.AutoOffsetReset.EARLIEST);
        }

        public static Consumed<K, V> with(ISerDes<K> keySerdes, ISerDes<V> valueSerdes, TimestampExtractor extractor, Topology.AutoOffsetReset autoOffsetReset)
        {
            return new Consumed<K, V>(keySerdes, valueSerdes, extractor, autoOffsetReset);
        }

        public static Consumed<K, V> with(TimestampExtractor extractor)
        {
            return new Consumed<K, V>(null, null, extractor, Topology.AutoOffsetReset.EARLIEST);
        }

        public static Consumed<K, V> with(Topology.AutoOffsetReset autoOffsetReset)
        {
            return new Consumed<K, V>(null, null, null, autoOffsetReset);
        }

        #endregion
    }
}
