using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Stream.Internal
{
    internal class ConsumedInternal<K, V>
    {
        internal ISerDes<K> KeySerdes { get; }
        internal ISerDes<V> ValueSerdes { get; }
        internal ITimestampExtractor TimestampExtractor { get; }

        public ConsumedInternal(
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes,
            ITimestampExtractor timestampExtractor)
        {
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            TimestampExtractor = timestampExtractor;
        }
    }
}
