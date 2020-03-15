using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal.Graph
{
    internal class KStreamFlatMapValues<K, V, VR> : IProcessorSupplier<K, V>
    {
        private IValueMapperWithKey<K, V, IEnumerable<VR>> Mapper { get; }

        public KStreamFlatMapValues(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
        {
            this.Mapper = mapper;
        }

        public IProcessor<K, V> Get() => new KStreamFlatMapValuesProcessor<K, V, VR>(this.Mapper);
    }
}
