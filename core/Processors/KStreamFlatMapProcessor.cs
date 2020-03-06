using kafka_stream_core.Stream.Internal.Graph;

namespace kafka_stream_core.Processors
{
    internal class KStreamFlatMapProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private KStreamFlatMap<K, V, K1, V1> kStreamFlatMap;

        public KStreamFlatMapProcessor(KStreamFlatMap<K, V, K1, V1> kStreamFlatMap)
        {
            this.kStreamFlatMap = kStreamFlatMap;
        }

        public override void Process(K key, V value)
        {
            foreach (var newPair in kStreamFlatMap.Mapper.apply(key, value))
                this.Forward(newPair.Key, newPair.Value);
        }
    }
}
