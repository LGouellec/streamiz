using kafka_stream_core.Stream.Internal.Graph;
using System.Collections.Generic;

namespace kafka_stream_core.Processors
{
    internal class KStreamMapProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private KStreamMap<K, V, K1, V1> kStreamMap;

        public KStreamMapProcessor(KStreamMap<K, V, K1, V1> kStreamMap)
        {
            this.kStreamMap = kStreamMap;
        }

        public override void Process(K key, V value)
        {
            KeyValuePair<K1, V1> newPair = new KeyValuePair<K1, V1>();
            if (kStreamMap.Mapper != null)
                newPair = kStreamMap.Mapper.apply(key, value);
            else
                newPair = kStreamMap.MapperFunction.Invoke(key, value);

            this.Forward(newPair.Key, newPair.Value);
        }
    }
}
