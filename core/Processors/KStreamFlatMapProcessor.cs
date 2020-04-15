using System.Collections.Generic;
using Kafka.Streams.Net.Stream;
using Kafka.Streams.Net.Stream.Internal.Graph;

namespace Kafka.Streams.Net.Processors
{
    internal class KStreamFlatMapProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private IKeyValueMapper<K, V, IEnumerable<KeyValuePair<K1, V1>>> mapper;

        public KStreamFlatMapProcessor(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<K1, V1>>> mapper)
        {
            this.mapper = mapper;
        }

        public override object Clone()
        {
            var p = new KStreamFlatMapProcessor<K, V, K1, V1>(this.mapper);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            foreach (var newPair in this.mapper.Apply(key, value))
                this.Forward(newPair.Key, newPair.Value);
        }
    }
}
