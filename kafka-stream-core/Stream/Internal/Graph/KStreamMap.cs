using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal.Graph
{
    public class KStreamMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        public KeyValueMapper<K, V, KeyValuePair<K1, V1>> Mapper { get; }
        public Func<K, V, KeyValuePair<K1, V1>> MapperFunction { get; }

        public KStreamMap(KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
            : this(mapper, null)
        {
        }

        public KStreamMap(Func<K, V, KeyValuePair<K1, V1>> mapperFunction)
            : this(null, mapperFunction)
        {
        }

        private KStreamMap(KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper, Func<K, V, KeyValuePair<K1, V1>> mapperFunction)
        {
            Mapper = mapper;
            MapperFunction = mapperFunction;
        }

        public IProcessor<K, V> Get() => new KStreamMapProcessor<K, V, K1, V1>(this);
    }
}
