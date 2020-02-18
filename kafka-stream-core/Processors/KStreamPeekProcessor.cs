using kafka_stream_core.Stream.Internal.Graph;

namespace kafka_stream_core.Processors
{
    internal class KStreamPeekProcessor<K, V> : AbstractProcessor<K, V>
    {
        private KStreamPeek<K, V> kStreamPeek;

        public KStreamPeekProcessor(KStreamPeek<K, V> kStreamPeek)
        {
            this.kStreamPeek = kStreamPeek;
        }

        public override void Process(K key, V value)
        {
            kStreamPeek.Action.Invoke(key, value);
            if (kStreamPeek.ForwardDownStream)
                this.Forward(key, value);
        }
    }
}
