using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Streams.Net.Stream;

namespace Kafka.Streams.Net.Processors
{
    internal class KStreamFlatMapValuesProcessor<K, V, VR> : AbstractProcessor<K, V>
    {
        private IValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValuesProcessor(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
        {
            this.mapper = mapper;
        }

        public override object Clone()
        {
            var p= new KStreamFlatMapValuesProcessor<K, V, VR>(this.mapper);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            foreach (var newValue in this.mapper.Apply(key, value))
                this.Forward(key, newValue);
        }
    }
}
