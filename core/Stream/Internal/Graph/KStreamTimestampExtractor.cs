using Streamiz.Kafka.Net.Processors;
using System;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamTimestampExtractor<K,V> : IProcessorSupplier<K, V>
    {
        private readonly Func<K, V, long> timestampExtractor;

        public KStreamTimestampExtractor(Func<K, V, long> timestampExtractor)
        {
            this.timestampExtractor = timestampExtractor;
        }

        public IProcessor<K, V> Get() => new KStreamTimestampProcessor<K,V>(timestampExtractor);
    }
}
