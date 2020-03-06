using kafka_stream_core.Processors;
using System.Collections.Generic;

namespace kafka_stream_core.Stream.Internal.Graph
{
    internal class KStreamFlatMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        public KeyValueMapper<K, V, IEnumerable<KeyValuePair<K1, V1>>> Mapper { get; }

        public KStreamFlatMap(KeyValueMapper<K, V, IEnumerable<KeyValuePair<K1, V1>>> mapper)
        {
            this.Mapper = mapper;
        }

        public IProcessor<K, V> Get() => new KStreamFlatMapProcessor<K, V, K1, V1>(this);
    }
}
