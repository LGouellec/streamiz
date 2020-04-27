using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamMapProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

        public KStreamMapProcessor(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
        {
            this.mapper = mapper;
        }


        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            KeyValuePair<K1, V1> newPair = this.mapper.Apply(key, value);
            this.Forward(newPair.Key, newPair.Value);
        }
    }
}
