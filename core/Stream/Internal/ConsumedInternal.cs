using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Stream.Internal
{
    internal class ConsumedInternal<K, V>
    {
        internal ISerDes<K> KeySerdes { get; }
        internal ISerDes<V> ValueSerdes { get; }
        internal ITimestampExtractor TimestampExtractor { get; }
        internal string Named { get; }

        public ConsumedInternal(
            string named,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes,
            ITimestampExtractor timestampExtractor)
        {
            Named = named;
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            TimestampExtractor = timestampExtractor;
        }
    }
}
