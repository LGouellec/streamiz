using Streamiz.Kafka.Net.Stream;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamFlatMapValuesProcessor<K, V, VR> : AbstractProcessor<K, V>
    {
        private readonly IValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValuesProcessor(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
        {
            this.mapper = mapper;
        }


        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            
            foreach (var newValue in this.mapper.Apply(key, value))
            {
                var originalHeader = Context.RecordContext.Headers.Clone();
                Forward(key, newValue);
                Context.SetHeaders(originalHeader);
            }
        }
    }
}
