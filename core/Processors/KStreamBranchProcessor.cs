using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamBranchProcessor<K, V> : AbstractProcessor<K, V>
    {
        private Func<K, V, bool>[] predicates;
        private string[] childNodes;

        public KStreamBranchProcessor(Func<K, V, bool>[] predicates, string[] childNodes)
        {
            this.predicates = predicates;
            this.childNodes = childNodes;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            for (int i = 0; i < this.predicates.Length; i++)
            {
                if (this.predicates[i].Invoke(key, value))
                {
                    this.Forward(key, value, this.childNodes[i]);
                    break;
                }
            }
        }
    }
}
