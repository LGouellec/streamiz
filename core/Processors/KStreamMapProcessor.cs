using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal.Graph;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Processors
{
    internal class KStreamMapProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;
        private Func<K, V, KeyValuePair<K1, V1>> mapperFunction;

        public KStreamMapProcessor(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper, Func<K, V, KeyValuePair<K1, V1>> mapperFunction)
        {
            this.mapper = mapper;
            this.mapperFunction = mapperFunction;
        }

        public override object Clone()
        {
            var p= new KStreamMapProcessor<K, V, K1, V1>(this.mapper, this.mapperFunction);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            KeyValuePair<K1, V1> newPair = new KeyValuePair<K1, V1>();
            if (this.mapper != null)
                newPair = this.mapper.apply(key, value);
            else
                newPair = this.mapperFunction.Invoke(key, value);

            this.Forward(newPair.Key, newPair.Value);
        }
    }
}
