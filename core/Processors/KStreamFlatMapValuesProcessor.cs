using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.Stream;

namespace kafka_stream_core.Processors
{
    internal class KStreamFlatMapValuesProcessor<K, V, VR> : AbstractProcessor<K, V>
    {
        private IValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValuesProcessor(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
        {
            this.mapper = mapper;
        }

        public override void Process(K key, V value)
        {
            foreach (var newValue in this.mapper.apply(key, value))
                this.Forward(key, newValue);
        }
    }
}
