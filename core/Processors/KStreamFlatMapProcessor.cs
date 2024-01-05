using Streamiz.Kafka.Net.Stream;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamFlatMapProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private readonly IKeyValueMapper<K, V, IEnumerable<KeyValuePair<K1, V1>>> mapper;

        public KStreamFlatMapProcessor(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<K1, V1>>> mapper)
        {
            this.mapper = mapper;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            var originalHeader = Context.RecordContext.Headers.Clone();

            foreach (var newPair in mapper.Apply(key, value))
            {
                Forward(newPair.Key, newPair.Value);
                Context.SetHeaders(originalHeader);
            }
        }
    }
}
