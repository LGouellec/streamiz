using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Stream.Internal
{
    internal class ConsumedInternal<K, V>
    {
        internal ISerDes<K> KeySerdes { get; }
        internal ISerDes<V> ValueSerdes { get; }
        internal ITimestampExtractor TimestampExtractor { get; }
        internal Topology.AutoOffsetReset AutoOffsetReset { get; }

        public ConsumedInternal(
            ISerDes<K> keySerdes,
            ISerDes<V> value,
            ITimestampExtractor timestampExtractor,
            Topology.AutoOffsetReset autoOffset)
        {
            KeySerdes = keySerdes;
            ValueSerdes = ValueSerdes;
            TimestampExtractor = timestampExtractor;
            AutoOffsetReset = autoOffset;
        }
    }
}
