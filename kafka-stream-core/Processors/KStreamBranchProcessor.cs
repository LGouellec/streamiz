using kafka_stream_core.Stream.Internal.Graph;

namespace kafka_stream_core.Processors
{
    internal class KStreamBranchProcessor<K, V> : AbstractProcessor<K, V>
    {
        private KStreamBranch<K, V> kStreamBranch;

        public KStreamBranchProcessor(KStreamBranch<K, V> kStreamBranch)
        {
            this.kStreamBranch = kStreamBranch;
        }

        public override void Process(K key, V value)
        {
            for (int i = 0; i < kStreamBranch.Predicates.Length; i++)
            {
                if (kStreamBranch.Predicates[i].Invoke(key, value))
                {
                    this.Forward(key, value, kStreamBranch.ChildNodes[i]);
                    break;
                }
            }
        }
    }
}
