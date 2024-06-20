using System;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class DefaultRecordTimestampExtractor<K, V> : IRecordTimestampExtractor<K, V>
    {
        private readonly Func<K, V, IRecordContext, long> timestampExtractor;

        public DefaultRecordTimestampExtractor()
        {
            this.timestampExtractor = (k, v, ctx) => ctx.Timestamp;
        }

        public long Extract(K key, V value, IRecordContext recordContext) => timestampExtractor(key, value, recordContext);
    }
}
