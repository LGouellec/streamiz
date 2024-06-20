using System;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class WrapperRecordTimestampExtractor<K, V> : IRecordTimestampExtractor<K, V>
    {
        private readonly Func<K, V, IRecordContext, long> timestampExtractor;

        public WrapperRecordTimestampExtractor(Func<K,V,IRecordContext,long> timestampExtractor)
        {
            this.timestampExtractor = timestampExtractor;
        }

        public long Extract(K key, V value, IRecordContext recordContext) => timestampExtractor(key, value ,recordContext);
    }
}
