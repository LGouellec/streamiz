using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal.Graph
{
    internal class KStreamMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        public IKeyValueMapper<K, V, KeyValuePair<K1, V1>> Mapper { get; }

        public KStreamMap(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
        {
            this.Mapper = mapper;
        }

        public IProcessor<K, V> Get() => new KStreamMapProcessor<K, V, K1, V1>(this.Mapper);
    }
}
