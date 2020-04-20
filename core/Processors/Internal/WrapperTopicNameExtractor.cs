using System;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class WrapperTopicNameExtractor<K, V> : ITopicNameExtractor<K, V>
    {
        private readonly Func<K, V, IRecordContext, string> inner;

        public WrapperTopicNameExtractor(Func<K, V, IRecordContext, string> inner)
        {
            this.inner = inner;
        }

        public string Extract(K key, V value, IRecordContext recordContext)
            => inner.Invoke(key, value, recordContext);
    }
}
